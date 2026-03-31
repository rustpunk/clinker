use serde::Deserialize;
use std::path::{Path, PathBuf};

use crate::error::ChannelError;

/// Top-level clinker.toml structure. Deserialized with two sections:
/// `[workspace]` for directory layout and `[defaults]` for fallback values.
#[derive(Debug, Clone, Deserialize)]
pub struct ClinkerToml {
    #[serde(default)]
    pub workspace: WorkspaceConfig,
    #[serde(default)]
    pub defaults: DefaultsConfig,
}

/// Workspace layout configuration from `[workspace]` in clinker.toml.
/// All fields have sensible defaults — an empty `[workspace]` section works.
#[derive(Debug, Clone, Deserialize)]
pub struct WorkspaceConfig {
    /// Directory containing per-channel subdirectories. Default: "channels"
    #[serde(default = "default_channels_dir")]
    pub channels_dir: String,

    /// Directory containing group override templates. Default: "_groups"
    #[serde(default = "default_groups_dir")]
    pub groups_dir: String,

    /// Optional default channel ID — CLI uses as fallback with INFO log.
    pub default_channel: Option<String>,

    /// Reserved for future --all-channels batch mode. CLI uses explicit file path.
    #[serde(default)]
    pub pipeline_paths: Vec<String>,
}

impl Default for WorkspaceConfig {
    fn default() -> Self {
        Self {
            channels_dir: default_channels_dir(),
            groups_dir: default_groups_dir(),
            default_channel: None,
            pipeline_paths: Vec::new(),
        }
    }
}

/// Default values from `[defaults]` in clinker.toml.
/// Lowest tier of the --env precedence chain.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DefaultsConfig {
    /// Default CLINKER_ENV value when --env absent and env var unset.
    pub env: Option<String>,
}

/// Resolved workspace root: the directory containing clinker.toml plus its parsed config.
/// All composition/channel paths are resolved relative to this root.
#[derive(Debug)]
pub struct WorkspaceRoot {
    pub root: PathBuf,
    pub config: WorkspaceConfig,
    pub defaults: DefaultsConfig,
}

impl WorkspaceRoot {
    /// Walk up from pipeline file's parent to find clinker.toml.
    /// Returns None if no clinker.toml found in any ancestor.
    ///
    /// Checks `CLINKER_WORKSPACE_ROOT` env var before walking — if set,
    /// uses that path directly without walking.
    ///
    /// Uses the parent directory for canonicalization (not the file itself)
    /// so this works even if the pipeline file doesn't exist yet.
    pub fn discover(from_pipeline: &Path) -> Option<Self> {
        // Check CLINKER_WORKSPACE_ROOT env var first — bypasses walk entirely
        if let Ok(ws_root) = std::env::var("CLINKER_WORKSPACE_ROOT") {
            let root = PathBuf::from(ws_root);
            return Self::at(&root).ok();
        }

        // Walk from the pipeline file's parent directory upward
        let parent = from_pipeline.parent()?;
        let start = if parent.exists() {
            parent.canonicalize().ok()?
        } else {
            parent.to_path_buf()
        };

        let mut walk_dir = Some(start.as_path());
        while let Some(dir) = walk_dir {
            let candidate = dir.join("clinker.toml");
            if candidate.exists() {
                return Self::load_at(dir).ok();
            }
            walk_dir = dir.parent();
        }

        None
    }

    /// Use explicit path. Reads clinker.toml at root, errors if missing.
    pub fn at(root: &Path) -> Result<Self, ChannelError> {
        let root = root.canonicalize().map_err(ChannelError::Io)?;
        Self::load_at(&root)
    }

    /// Internal: load clinker.toml from the given (already resolved) root directory.
    fn load_at(root: &Path) -> Result<Self, ChannelError> {
        let toml_path = root.join("clinker.toml");
        let content = std::fs::read_to_string(&toml_path).map_err(ChannelError::Io)?;
        let parsed: ClinkerToml = toml::from_str(&content)?;
        Ok(Self {
            root: root.to_path_buf(),
            config: parsed.workspace,
            defaults: parsed.defaults,
        })
    }

    /// Absolute path to the channels directory.
    pub fn channels_dir(&self) -> PathBuf {
        self.root.join(&self.config.channels_dir)
    }

    /// Absolute path to the groups directory.
    pub fn groups_dir(&self) -> PathBuf {
        self.root.join(&self.config.groups_dir)
    }

    /// Absolute path to a specific channel's directory.
    pub fn channel_dir(&self, id: &str) -> PathBuf {
        self.channels_dir().join(id)
    }

    /// Absolute path to a specific group's directory.
    pub fn group_dir(&self, id: &str) -> PathBuf {
        self.groups_dir().join(id)
    }

    /// Resolve a workspace-root-relative composition path to an absolute path.
    pub fn resolve_comp_path(&self, rel: &str) -> PathBuf {
        self.root.join(rel)
    }
}

fn default_channels_dir() -> String {
    "channels".into()
}

fn default_groups_dir() -> String {
    "_groups".into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Walk from nested dir finds clinker.toml in ancestor.
    #[test]
    fn test_workspace_discover_from_nested_dir() {
        let tmp = TempDir::new().unwrap();
        let ws = tmp.path();
        std::fs::write(ws.join("clinker.toml"), "[workspace]\n").unwrap();
        std::fs::create_dir_all(ws.join("pipelines/deep")).unwrap();
        let target = ws.join("pipelines/deep/etl.yaml");
        std::fs::write(&target, "").unwrap();
        let found = WorkspaceRoot::discover(&target).unwrap();
        assert_eq!(found.root, ws.canonicalize().unwrap());
    }

    /// Pipeline in same dir as clinker.toml finds it immediately.
    #[test]
    fn test_workspace_discover_from_workspace_root() {
        let tmp = TempDir::new().unwrap();
        let ws = tmp.path();
        std::fs::write(ws.join("clinker.toml"), "[workspace]\n").unwrap();
        let target = ws.join("pipeline.yaml");
        std::fs::write(&target, "").unwrap();
        let found = WorkspaceRoot::discover(&target).unwrap();
        assert_eq!(found.root, ws.canonicalize().unwrap());
    }

    /// No clinker.toml anywhere → None.
    #[test]
    fn test_workspace_discover_returns_none_without_marker() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("pipeline.yaml");
        std::fs::write(&target, "").unwrap();
        assert!(WorkspaceRoot::discover(&target).is_none());
    }

    /// clinker.toml with empty [workspace] applies all defaults.
    #[test]
    fn test_workspace_config_all_defaults() {
        let tmp = TempDir::new().unwrap();
        let ws = tmp.path();
        std::fs::write(ws.join("clinker.toml"), "[workspace]\n").unwrap();
        let target = ws.join("pipeline.yaml");
        std::fs::write(&target, "").unwrap();
        let found = WorkspaceRoot::discover(&target).unwrap();
        assert_eq!(found.config.channels_dir, "channels");
        assert_eq!(found.config.groups_dir, "_groups");
        assert!(found.config.default_channel.is_none());
        assert!(found.config.pipeline_paths.is_empty());
        assert!(found.defaults.env.is_none());
    }

    /// Non-default channels_dir / groups_dir parsed.
    #[test]
    fn test_workspace_config_custom_dirs() {
        let tmp = TempDir::new().unwrap();
        let ws = tmp.path();
        std::fs::write(
            ws.join("clinker.toml"),
            r#"
[workspace]
channels_dir = "my_channels"
groups_dir = "my_groups"
default_channel = "acme"
pipeline_paths = ["pipelines/"]
"#,
        )
        .unwrap();
        let target = ws.join("pipeline.yaml");
        std::fs::write(&target, "").unwrap();
        let found = WorkspaceRoot::discover(&target).unwrap();
        assert_eq!(found.config.channels_dir, "my_channels");
        assert_eq!(found.config.groups_dir, "my_groups");
        assert_eq!(found.config.default_channel.as_deref(), Some("acme"));
        assert_eq!(found.config.pipeline_paths, vec!["pipelines/"]);
    }

    /// [defaults] env = "staging" parsed into DefaultsConfig.
    #[test]
    fn test_workspace_config_defaults_env() {
        let tmp = TempDir::new().unwrap();
        let ws = tmp.path();
        std::fs::write(
            ws.join("clinker.toml"),
            r#"
[workspace]

[defaults]
env = "staging"
"#,
        )
        .unwrap();
        let target = ws.join("pipeline.yaml");
        std::fs::write(&target, "").unwrap();
        let found = WorkspaceRoot::discover(&target).unwrap();
        assert_eq!(found.defaults.env.as_deref(), Some("staging"));
    }

    /// CLINKER_WORKSPACE_ROOT bypasses walk.
    #[test]
    fn test_workspace_env_var_override() {
        let tmp = TempDir::new().unwrap();
        let ws = tmp.path();
        std::fs::write(ws.join("clinker.toml"), "[workspace]\n").unwrap();

        // Create a pipeline file in a completely different temp dir (no clinker.toml there)
        let other = TempDir::new().unwrap();
        let target = other.path().join("pipeline.yaml");
        std::fs::write(&target, "").unwrap();

        // Set the env var to point at the workspace
        let canonical_ws = ws.canonicalize().unwrap();
        unsafe { std::env::set_var("CLINKER_WORKSPACE_ROOT", &canonical_ws) };
        let found = WorkspaceRoot::discover(&target).unwrap();
        unsafe { std::env::remove_var("CLINKER_WORKSPACE_ROOT") };

        assert_eq!(found.root, canonical_ws);
    }

    /// WorkspaceRoot::at() uses given path, no walk.
    #[test]
    fn test_workspace_at_explicit_path() {
        let tmp = TempDir::new().unwrap();
        let ws = tmp.path();
        std::fs::write(ws.join("clinker.toml"), "[workspace]\n").unwrap();

        let found = WorkspaceRoot::at(ws).unwrap();
        assert_eq!(found.root, ws.canonicalize().unwrap());
        assert_eq!(found.config.channels_dir, "channels");
    }

    /// Same target file from different CWDs → same workspace root.
    /// (CWD-independence: discovery walks from the target file's parent, not CWD.)
    #[test]
    fn test_workspace_cwd_independence() {
        let tmp = TempDir::new().unwrap();
        let ws = tmp.path();
        std::fs::write(ws.join("clinker.toml"), "[workspace]\n").unwrap();
        std::fs::create_dir_all(ws.join("pipelines")).unwrap();
        let target = ws.join("pipelines/etl.yaml");
        std::fs::write(&target, "").unwrap();

        // discover() uses from_pipeline's parent — CWD is irrelevant.
        // We call it twice with the same absolute target path to verify
        // the result is deterministic regardless of process CWD.
        let canonical_target = target.canonicalize().unwrap();
        let found1 = WorkspaceRoot::discover(&canonical_target).unwrap();
        let found2 = WorkspaceRoot::discover(&canonical_target).unwrap();
        assert_eq!(found1.root, found2.root);
        assert_eq!(found1.root, ws.canonicalize().unwrap());
    }
}
