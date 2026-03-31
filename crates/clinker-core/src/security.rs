use std::path::{Path, PathBuf};

use crate::config::PipelineConfig;

/// Error from security validation.
#[derive(Debug)]
pub struct SecurityError {
    pub message: String,
}

impl std::fmt::Display for SecurityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "security error: {}", self.message)
    }
}

impl std::error::Error for SecurityError {}

/// Validate a path from pipeline config.
/// Rejects directory traversal (..), absolute paths (unless allowed),
/// null bytes, and resolves relative paths against base_dir.
pub fn validate_path(
    path: &Path,
    base_dir: &Path,
    allow_absolute: bool,
) -> Result<PathBuf, SecurityError> {
    let path_str = path.to_string_lossy();

    // Reject null bytes
    if path_str.contains('\0') {
        return Err(SecurityError {
            message: format!("path contains null byte: {:?}", path),
        });
    }

    // Reject URL-encoded traversal
    if path_str.contains("%2e%2e") || path_str.contains("%2E%2E") {
        return Err(SecurityError {
            message: format!("path contains encoded directory traversal: {:?}", path),
        });
    }

    // Reject directory traversal (..)
    for component in path.components() {
        if let std::path::Component::ParentDir = component {
            return Err(SecurityError {
                message: format!("path contains directory traversal (..): {:?}", path),
            });
        }
    }

    // Reject absolute paths unless flag is set
    if path.is_absolute() && !allow_absolute {
        return Err(SecurityError {
            message: format!(
                "absolute path not allowed: {:?} — use --allow-absolute-paths to override",
                path
            ),
        });
    }

    // Resolve relative paths against base_dir
    if path.is_relative() {
        Ok(base_dir.join(path))
    } else {
        Ok(path.to_path_buf())
    }
}

/// Check if output file exists (overwrite protection).
/// Returns Ok(()) if the file doesn't exist or overwrite is allowed.
pub fn check_overwrite(
    path: &Path,
    force: bool,
    config_overwrite: bool,
) -> Result<(), SecurityError> {
    if path.exists() && !force && !config_overwrite {
        return Err(SecurityError {
            message: format!(
                "output file already exists: {:?} — use --force or set overwrite: true to overwrite",
                path
            ),
        });
    }
    Ok(())
}

/// Validate all paths in a PipelineConfig.
pub fn validate_all_config_paths(
    config: &PipelineConfig,
    base_dir: &Path,
    allow_absolute: bool,
) -> Result<(), SecurityError> {
    // Validate input paths
    for input in &config.inputs {
        validate_path(Path::new(&input.path), base_dir, allow_absolute)?;
    }

    // Validate output paths
    for output in &config.outputs {
        validate_path(Path::new(&output.path), base_dir, allow_absolute)?;
    }

    // Validate rules_path if set
    if let Some(ref rules_path) = config.pipeline.rules_path {
        validate_path(Path::new(rules_path), base_dir, allow_absolute)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn base() -> PathBuf {
        PathBuf::from("/opt/pipelines")
    }

    #[test]
    fn test_path_reject_dotdot() {
        let result = validate_path(Path::new("../etc/passwd"), &base(), false);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("directory traversal"));
    }

    #[test]
    fn test_path_reject_absolute() {
        let result = validate_path(Path::new("/tmp/out.csv"), &base(), false);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("absolute path not allowed"));
    }

    #[test]
    fn test_path_allow_absolute_flag() {
        let result = validate_path(Path::new("/tmp/out.csv"), &base(), true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), PathBuf::from("/tmp/out.csv"));
    }

    #[test]
    fn test_path_base_dir_resolution() {
        let result = validate_path(Path::new("data/input.csv"), &base(), false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), PathBuf::from("/opt/pipelines/data/input.csv"));
    }

    #[test]
    fn test_path_base_dir_default() {
        let yaml_dir = PathBuf::from("/home/user/configs");
        let result = validate_path(Path::new("data/input.csv"), &yaml_dir, false);
        assert_eq!(result.unwrap(), PathBuf::from("/home/user/configs/data/input.csv"));
    }

    #[test]
    fn test_path_reject_encoded_dotdot() {
        let result = validate_path(Path::new("%2e%2e/etc/passwd"), &base(), false);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("encoded directory traversal"));
    }

    #[test]
    fn test_path_dot_slash_prefix() {
        let result = validate_path(Path::new("./data/input.csv"), &base(), false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), PathBuf::from("/opt/pipelines/./data/input.csv"));
    }

    #[test]
    fn test_path_null_bytes() {
        let result = validate_path(Path::new("data/input\0.csv"), &base(), false);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("null byte"));
    }

    #[test]
    fn test_path_validation_all_config_paths() {
        // Minimal valid config with relative paths
        let yaml = r#"
pipeline:
  name: test
  rules_path: ./rules

inputs:
  - name: src
    type: csv
    path: data/input.csv
outputs:
  - name: dest
    type: csv
    path: output/result.csv
transformations:
  - name: t1
    cxl: "emit x = a"
"#;
        let config = crate::config::parse_config(yaml).unwrap();
        let result = validate_all_config_paths(&config, &base(), false);
        assert!(result.is_ok());
    }

    // ── Overwrite protection tests ────────────────────────────────

    #[test]
    fn test_overwrite_refuse_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("existing.csv");
        fs::write(&path, "data").unwrap();

        let result = check_overwrite(&path, false, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("already exists"));
    }

    #[test]
    fn test_overwrite_force_flag() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("existing.csv");
        fs::write(&path, "data").unwrap();

        let result = check_overwrite(&path, true, false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_overwrite_config_flag() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("existing.csv");
        fs::write(&path, "data").unwrap();

        let result = check_overwrite(&path, false, true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_overwrite_nonexistent_ok() {
        let result = check_overwrite(Path::new("/tmp/nonexistent_clinker_test.csv"), false, false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_overwrite_check_before_processing() {
        // Verify the check can be called before any I/O
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        fs::write(&path, "existing").unwrap();
        let err = check_overwrite(&path, false, false).unwrap_err();
        assert!(err.message.contains("--force"));
    }

    #[test]
    fn test_overwrite_multiple_outputs_all_checked() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("a.csv");
        let p2 = dir.path().join("b.csv");
        fs::write(&p1, "data").unwrap();
        // p2 doesn't exist

        assert!(check_overwrite(&p1, false, false).is_err());
        assert!(check_overwrite(&p2, false, false).is_ok());
    }
}
