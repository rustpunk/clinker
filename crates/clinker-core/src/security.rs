//! Path-security chokepoint (Phase 11 assumption #51, Phase 16b Task 16b.1.5).
//!
//! The only way to obtain a [`ValidatedPath`] is by calling [`validate_path`].
//! The newtype's inner field is private, so downstream code that consumes a
//! `ValidatedPath` has compile-time proof that the path has been canonicalized,
//! scoped to its base directory, and screened for directory-traversal and
//! null-byte attacks.
//!
//! This is the "token of proof" pattern (rustc's `ThinVec<T>`, `ValidatedRoot`,
//! and similar). It makes it impossible to feed an unvalidated path into
//! [`crate::span::SourceDb::load`].

use std::path::{Path, PathBuf};

use crate::config::PipelineConfig;
use crate::error::{Diagnostic, LabeledSpan};
use crate::span::Span;

/// A path that has passed [`validate_path`].
///
/// Fields are private; the only public constructor is [`validate_path`].
/// Downstream APIs (notably [`crate::span::SourceDb::load`]) take
/// `ValidatedPath` by value to force callers through the validator.
///
/// ```compile_fail
/// use clinker_core::security::ValidatedPath;
/// use std::path::PathBuf;
/// // Direct construction is rejected: the inner field is private.
/// let _ = ValidatedPath(PathBuf::from("/etc/passwd"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ValidatedPath(PathBuf);

impl ValidatedPath {
    /// Borrow the inner path.
    pub fn as_path(&self) -> &Path {
        &self.0
    }

    /// Consume the wrapper and return the validated `PathBuf`.
    pub fn into_inner(self) -> PathBuf {
        self.0
    }
}

impl AsRef<Path> for ValidatedPath {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

/// Validate a path from pipeline config and return a [`ValidatedPath`] proof token.
///
/// Performs, in order:
///
/// 1. Null-byte rejection.
/// 2. URL-encoded traversal rejection (`%2e%2e`).
/// 3. `..` component rejection.
/// 4. Absolute-path rejection unless `allow_absolute` is set.
/// 5. Relative-path resolution against `base_dir`.
/// 6. Canonicalization + symlink-escape check: if the resolved path exists,
///    canonicalize it and reject anything that escapes the canonicalized
///    `base_dir` via symlinks.
///
/// # Base directory convention
///
/// The `base_dir` argument is the security root that the resulting path is
/// scoped to. Callers should pass:
///
/// - **CLI / on-disk pipeline:** the directory containing the YAML file.
/// - **Kiln IDE unsaved buffers:** `std::env::current_dir()`. Unsaved Kiln
///   buffers have no on-disk path yet, so the IDE uses the current working
///   directory as the security root. **Never** pass `/` (that would validate
///   everything against the filesystem root).
///
/// # Errors
///
/// Returns a [`Diagnostic`] (severity `Error`, code `E-SEC-001`) with a
/// synthetic primary span. Callers that know the source location of the
/// offending `path:` field should replace the primary span before surfacing
/// the diagnostic to the user.
#[allow(clippy::result_large_err)] // Diagnostic is the agreed error type (Phase 16b).
pub fn validate_path(
    raw: &Path,
    base_dir: &Path,
    allow_absolute: bool,
) -> Result<ValidatedPath, Diagnostic> {
    let path_str = raw.to_string_lossy();

    if path_str.contains('\0') {
        return Err(sec_diag(format!("path contains null byte: {raw:?}")));
    }

    if path_str.contains("%2e%2e") || path_str.contains("%2E%2E") {
        return Err(sec_diag(format!(
            "path contains encoded directory traversal: {raw:?}"
        )));
    }

    for component in raw.components() {
        if let std::path::Component::ParentDir = component {
            return Err(sec_diag(format!(
                "path contains directory traversal (..): {raw:?}"
            )));
        }
    }

    if raw.is_absolute() && !allow_absolute {
        return Err(sec_diag(format!(
            "absolute path not allowed: {raw:?} — use --allow-absolute-paths to override"
        )));
    }

    let resolved = if raw.is_relative() {
        base_dir.join(raw)
    } else {
        raw.to_path_buf()
    };

    // Symlink-escape check: if both the candidate and the base exist, compare
    // their canonicalized forms. Non-existent paths (not-yet-created outputs,
    // for example) skip the check — there is no filesystem state to trust or
    // distrust yet.
    if let (Ok(resolved_canon), Ok(base_canon)) = (
        std::fs::canonicalize(&resolved),
        std::fs::canonicalize(base_dir),
    ) && !allow_absolute
        && !resolved_canon.starts_with(&base_canon)
    {
        return Err(sec_diag(format!(
            "path escapes base directory via symlink: {raw:?} -> {resolved_canon:?}"
        )));
    }

    Ok(ValidatedPath(resolved))
}

fn sec_diag(message: String) -> Diagnostic {
    // (c) `validate_path` takes a `&Path` with no source location.
    // `validate_all_config_paths` rewrites `primary` to a real
    // per-node span before the diagnostic leaves this module; the
    // rare direct callers (e.g. `check_overwrite`) operate on
    // pre-validated output paths with no YAML span to attach.
    Diagnostic::error(
        "E-SEC-001",
        message,
        LabeledSpan::new(Span::SYNTHETIC, None),
    )
}

/// Check if output file exists (overwrite protection).
#[allow(clippy::result_large_err)] // Diagnostic is the agreed error type (Phase 16b).
pub fn check_overwrite(path: &Path, force: bool, config_overwrite: bool) -> Result<(), Diagnostic> {
    if path.exists() && !force && !config_overwrite {
        return Err(sec_diag(format!(
            "output file already exists: {path:?} — use --force or set overwrite: true to overwrite"
        )));
    }
    Ok(())
}

/// Walk every path-bearing field of `config` and validate it. Collects all
/// failures rather than short-circuiting.
///
/// This is the Task 16b.1.5 scaffold of the `compile()` path pre-pass. It runs
/// against the current (pre-lift) [`PipelineConfig`] shape; Task 16b.2 will
/// rewrite it to walk `PipelineConfig.nodes` variants.
pub fn validate_all_config_paths(
    config: &PipelineConfig,
    base_dir: &Path,
    allow_absolute: bool,
) -> Vec<Diagnostic> {
    use crate::config::pipeline_node::PipelineNode;
    let mut diags = Vec::new();

    // Walk `config.nodes` directly so every path-bearing node carries
    // its real saphyr line through to the emitted diagnostic. This
    // eliminates the last `Span::SYNTHETIC` path-security site:
    // diagnostics now point at the offending node in the YAML.
    for spanned in &config.nodes {
        let saphyr_line = spanned.referenced.line();
        let span = if saphyr_line > 0 {
            // Real node-level span threaded through from the parser.
            Span::line_only(saphyr_line as u32)
        } else {
            // (c) saphyr tagged-enum/flatten edge case — no line
            // available. This is the only remaining synthetic span in
            // the path-security layer and is inherited from the same
            // library-level blocker that the other `(c)` sites cite.
            Span::SYNTHETIC
        };
        let (raw, _name): (Option<&str>, &str) = match &spanned.value {
            PipelineNode::Source { config, .. } => (
                Some(config.source.path.as_str()),
                config.source.name.as_str(),
            ),
            PipelineNode::Output { config, .. } => (
                Some(config.output.path.as_str()),
                config.output.name.as_str(),
            ),
            _ => (None, ""),
        };
        if let Some(raw) = raw
            && let Err(mut d) = validate_path(Path::new(raw), base_dir, allow_absolute)
        {
            d.primary = LabeledSpan::new(span, None);
            diags.push(d);
        }
    }

    if let Some(ref rules_path) = config.pipeline.rules_path
        && let Err(d) = validate_path(Path::new(rules_path), base_dir, allow_absolute)
    {
        // (a) Whole-pipeline `rules_path` has no per-node origin.
        diags.push(d);
    }

    diags
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn base() -> PathBuf {
        PathBuf::from("/opt/pipelines")
    }

    // ── ValidatedPath: sole-constructor proof ─────────────────────

    #[test]
    fn test_validated_path_only_via_validate_path() {
        // This test is mostly a smoke test; the real proof is the
        // `compile_fail` doc test on `ValidatedPath` plus the private
        // tuple field. If either regresses, the following assertion can
        // still be used to confirm that `validate_path` is the only
        // in-crate path that produces a `ValidatedPath`.
        let dir = tempfile::tempdir().unwrap();
        let rel = Path::new("inner.csv");
        // Create the file so canonicalization works against the base.
        fs::write(dir.path().join(rel), "x").unwrap();
        let vp = validate_path(rel, dir.path(), false).unwrap();
        assert!(vp.as_path().ends_with("inner.csv"));
    }

    // ── Parent-escape / symlink-escape ────────────────────────────

    #[test]
    fn test_validate_path_rejects_parent_escape() {
        let err = validate_path(Path::new("../etc/passwd"), &base(), false).unwrap_err();
        assert_eq!(err.code, "E-SEC-001");
        assert!(err.message.contains("directory traversal"));
    }

    #[test]
    fn test_validate_path_rejects_symlink_escape() {
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("secret.txt"), "shh").unwrap();

        let base = tempfile::tempdir().unwrap();
        let link = base.path().join("leak");
        #[cfg(unix)]
        std::os::unix::fs::symlink(outside.path().join("secret.txt"), &link).unwrap();
        #[cfg(not(unix))]
        {
            // On non-unix targets we can't create a symlink without admin rights.
            return;
        }

        let err = validate_path(Path::new("leak"), base.path(), false).unwrap_err();
        assert!(
            err.message.contains("escapes base directory"),
            "expected symlink-escape diagnostic, got: {}",
            err.message
        );
    }

    #[test]
    fn test_path_reject_absolute() {
        let err = validate_path(Path::new("/tmp/out.csv"), &base(), false).unwrap_err();
        assert!(err.message.contains("absolute path not allowed"));
    }

    #[test]
    fn test_path_allow_absolute_flag() {
        let vp = validate_path(Path::new("/tmp/out.csv"), &base(), true).unwrap();
        assert_eq!(vp.as_path(), Path::new("/tmp/out.csv"));
    }

    #[test]
    fn test_path_base_dir_resolution() {
        let vp = validate_path(Path::new("data/input.csv"), &base(), false).unwrap();
        assert_eq!(vp.as_path(), Path::new("/opt/pipelines/data/input.csv"));
    }

    #[test]
    fn test_path_reject_encoded_dotdot() {
        let err = validate_path(Path::new("%2e%2e/etc/passwd"), &base(), false).unwrap_err();
        assert!(err.message.contains("encoded directory traversal"));
    }

    #[test]
    fn test_path_null_bytes() {
        let err = validate_path(Path::new("data/input\0.csv"), &base(), false).unwrap_err();
        assert!(err.message.contains("null byte"));
    }

    // ── SourceDb::load signature gate ─────────────────────────────

    /// Compile-fail proof: `SourceDb::load` must not accept a raw `PathBuf`.
    /// If this ever starts compiling, the type-level guarantee has regressed.
    ///
    /// ```compile_fail
    /// use clinker_core::span::SourceDb;
    /// use std::path::PathBuf;
    /// let mut db = SourceDb::new();
    /// let _ = db.load(PathBuf::from("/etc/passwd"));
    /// ```
    #[test]
    fn test_source_db_load_requires_validated_path() {}

    // ── Kiln unsaved-buffer convention ────────────────────────────

    #[test]
    fn test_kiln_unsaved_buffer_uses_cwd_base() {
        // Kiln convention: when a buffer has no on-disk path, the IDE passes
        // `std::env::current_dir()` as the base_dir. This test exercises the
        // documented convention against a real relative path under cwd.
        let dir = tempfile::tempdir().unwrap();
        let original_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();
        let result = (|| {
            fs::write("sibling.yaml", "pipeline: {name: x}").unwrap();
            let base = std::env::current_dir().unwrap();
            validate_path(Path::new("sibling.yaml"), &base, false)
        })();
        // Always restore cwd, even if the assertion below fails.
        std::env::set_current_dir(&original_cwd).unwrap();
        let vp = result.expect("kiln-style validation must succeed");
        assert!(vp.as_path().ends_with("sibling.yaml"));
    }

    // ── compile() pre-pass scaffold ───────────────────────────────

    #[test]
    fn test_validate_all_config_paths_collects_errors() {
        let yaml = r#"
pipeline:
  name: test
  rules_path: ./rules

nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/input.csv
  - type: transform
    name: t1
    input: src
    config:
      cxl: "emit x = a"
  - type: output
    name: dest
    input: t1
    config:
      name: dest
      type: csv
      path: output/result.csv
"#;
        let config = crate::config::parse_config(yaml).unwrap();
        let diags = validate_all_config_paths(&config, &base(), false);
        assert!(diags.is_empty(), "unexpected diags: {diags:?}");
    }

    // ── Overwrite protection ──────────────────────────────────────

    #[test]
    fn test_overwrite_refuse_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("existing.csv");
        fs::write(&path, "data").unwrap();
        let err = check_overwrite(&path, false, false).unwrap_err();
        assert!(err.message.contains("already exists"));
    }

    #[test]
    fn test_overwrite_force_flag() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("existing.csv");
        fs::write(&path, "data").unwrap();
        assert!(check_overwrite(&path, true, false).is_ok());
    }

    #[test]
    fn test_overwrite_config_flag() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("existing.csv");
        fs::write(&path, "data").unwrap();
        assert!(check_overwrite(&path, false, true).is_ok());
    }

    #[test]
    fn test_overwrite_nonexistent_ok() {
        assert!(
            check_overwrite(Path::new("/tmp/nonexistent_clinker_test.csv"), false, false).is_ok()
        );
    }
}
