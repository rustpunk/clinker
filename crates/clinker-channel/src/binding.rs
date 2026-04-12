//! Channel binding types and YAML parser.
//!
//! A `.channel.yaml` file declares a named channel that targets a pipeline
//! or composition and overlays config/resource values onto its parameters.
//! Parsing computes a BLAKE3 content hash from the raw file bytes for
//! cache-invalidation identity (LD-16c-18).

use std::path::{Path, PathBuf};

use indexmap::IndexMap;
use serde::Deserialize;

use crate::error::ChannelError;

// ── DottedPath ──────────────────────────────────────────────────────────

/// A validated dotted path into a composition's declared config or resource
/// schema. At most two segments: `alias.param_name` (for composition
/// targets) or `param_name` alone (for pipeline-level config).
///
/// Rejects: empty strings, leading/trailing dots, consecutive dots, more
/// than 2 segments, and characters outside `[a-zA-Z0-9_.]`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DottedPath(String);

impl DottedPath {
    /// Return the inner string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Split into (alias, param_name) if two segments, or (None, param_name)
    /// if one segment.
    pub fn segments(&self) -> (Option<&str>, &str) {
        match self.0.split_once('.') {
            Some((alias, param)) => (Some(alias), param),
            None => (None, &self.0),
        }
    }
}

impl TryFrom<&str> for DottedPath {
    type Error = ChannelError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        if s.is_empty() {
            return Err(ChannelError::InvalidDottedPath {
                path: s.to_string(),
                reason: "path is empty".to_string(),
            });
        }
        if s.starts_with('.') || s.ends_with('.') {
            return Err(ChannelError::InvalidDottedPath {
                path: s.to_string(),
                reason: "path must not start or end with '.'".to_string(),
            });
        }
        if s.contains("..") {
            return Err(ChannelError::InvalidDottedPath {
                path: s.to_string(),
                reason: "consecutive dots are not allowed".to_string(),
            });
        }
        if !s
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.')
        {
            return Err(ChannelError::InvalidDottedPath {
                path: s.to_string(),
                reason: "only [a-zA-Z0-9_.] characters are allowed".to_string(),
            });
        }
        let segment_count = s.split('.').count();
        if segment_count > 2 {
            return Err(ChannelError::InvalidDottedPath {
                path: s.to_string(),
                reason: format!("at most 2 segments allowed, got {segment_count}"),
            });
        }
        Ok(DottedPath(s.to_string()))
    }
}

impl std::fmt::Display for DottedPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// ── ChannelTarget ───────────────────────────────────────────────────────

/// What a channel targets: either a pipeline or a composition.
#[derive(Debug, Clone)]
pub enum ChannelTarget {
    Pipeline(PathBuf),
    Composition(PathBuf),
}

// ── ChannelBinding ──────────────────────────────────────────────────────

/// A parsed `.channel.yaml` file.
///
/// The `config_default` / `config_fixed` split maps to `LayerKind::ChannelDefault`
/// and `LayerKind::ChannelFixed` respectively (LD-16c-16). Resources follow
/// the same split pattern.
#[derive(Debug, Clone)]
pub struct ChannelBinding {
    pub name: String,
    pub target: ChannelTarget,
    pub config_default: IndexMap<DottedPath, serde_json::Value>,
    pub config_fixed: IndexMap<DottedPath, serde_json::Value>,
    pub resources_default: IndexMap<DottedPath, serde_json::Value>,
    pub resources_fixed: IndexMap<DottedPath, serde_json::Value>,
    pub source_path: PathBuf,
    /// BLAKE3 hash of the raw file bytes (LD-16c-18).
    pub channel_hash: [u8; 32],
}

impl ChannelBinding {
    /// Parse a `.channel.yaml` from raw bytes, computing the BLAKE3 content
    /// hash in the same pass. The `source_path` is stored on the binding
    /// for diagnostic reporting.
    pub fn from_yaml_bytes(bytes: &[u8], source_path: PathBuf) -> Result<Self, ChannelError> {
        let channel_hash = blake3::hash(bytes).into();
        let text = std::str::from_utf8(bytes).map_err(|e| ChannelError::Utf8 {
            path: source_path.clone(),
            source: e,
        })?;
        let raw: RawChannelFile =
            clinker_core::yaml::from_str(text).map_err(|e| ChannelError::Yaml {
                path: source_path.clone(),
                source: Box::new(e.0),
            })?;

        let target = classify_target(&raw.channel.target);

        let config_default = validate_dotted_keys(raw.config.default)?;
        let config_fixed = validate_dotted_keys(raw.config.fixed)?;
        let resources_default = validate_dotted_keys(raw.resources.default)?;
        let resources_fixed = validate_dotted_keys(raw.resources.fixed)?;

        Ok(ChannelBinding {
            name: raw.channel.name,
            target,
            config_default,
            config_fixed,
            resources_default,
            resources_fixed,
            source_path,
            channel_hash,
        })
    }

    /// Load a `.channel.yaml` file from disk.
    pub fn load(path: &Path) -> Result<Self, ChannelError> {
        let bytes = std::fs::read(path)?;
        Self::from_yaml_bytes(&bytes, path.to_path_buf())
    }
}

// ── Serde intermediate types ────────────────────────────────────────────

#[derive(Deserialize)]
struct RawChannelFile {
    channel: RawChannelMeta,
    #[serde(default)]
    config: RawChannelConfig,
    #[serde(default)]
    resources: RawChannelConfig,
}

#[derive(Deserialize)]
struct RawChannelMeta {
    name: String,
    target: String,
}

#[derive(Deserialize, Default)]
struct RawChannelConfig {
    #[serde(default)]
    default: IndexMap<String, serde_json::Value>,
    #[serde(default)]
    fixed: IndexMap<String, serde_json::Value>,
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn classify_target(target: &str) -> ChannelTarget {
    if target.ends_with(".comp.yaml") {
        ChannelTarget::Composition(PathBuf::from(target))
    } else {
        ChannelTarget::Pipeline(PathBuf::from(target))
    }
}

fn validate_dotted_keys(
    raw: IndexMap<String, serde_json::Value>,
) -> Result<IndexMap<DottedPath, serde_json::Value>, ChannelError> {
    raw.into_iter()
        .map(|(k, v)| {
            let path = DottedPath::try_from(k.as_str())?;
            Ok((path, v))
        })
        .collect()
}
