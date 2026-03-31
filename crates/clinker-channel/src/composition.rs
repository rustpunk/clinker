use std::collections::HashMap;
use std::path::{Path, PathBuf};

use clinker_core::config::TransformConfig;
use serde::Deserialize;

use crate::error::ChannelError;
use crate::workspace::WorkspaceRoot;

/// Maps transform name → source `.comp.yaml` path (workspace-root-relative, canonical).
/// Transforms not from a composition are absent from this map.
pub type ProvenanceMap = HashMap<String, PathBuf>;

/// A parsed `.comp.yaml` composition file.
#[derive(Debug, Deserialize)]
pub struct CompositionFile {
    #[serde(rename = "_composition")]
    pub header: CompositionHeader,
    #[serde(default)]
    pub transformations: Vec<TransformConfig>,
}

/// Header section of a `.comp.yaml` file.
#[derive(Debug, Deserialize)]
pub struct CompositionHeader {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
}

impl CompositionFile {
    /// Load and parse a `.comp.yaml` file.
    pub fn load(path: &Path) -> Result<Self, ChannelError> {
        let raw = std::fs::read_to_string(path).map_err(|_| ChannelError::CompositionNotFound {
            path: path.to_path_buf(),
        })?;
        let parsed: CompositionFile = serde_saphyr::from_str(&raw)?;
        Ok(parsed)
    }
}

/// Resolve a list of transforms from a composition file, handling any nested
/// imports. Returns flat `Vec<TransformConfig>` and populates provenance.
///
/// This is a simplified version used by `add_compositions` in overrides.
/// The full `resolve_compositions()` for base pipeline `_import` directives
/// is implemented in Task 10.5.
pub fn resolve_composition_transforms(
    transforms: Vec<TransformConfig>,
    _workspace: &WorkspaceRoot,
    _channel_vars: &[(&str, &str)],
    _provenance: &mut ProvenanceMap,
) -> Result<Vec<TransformConfig>, ChannelError> {
    // For now, compositions loaded via add_compositions don't contain nested _import.
    // Task 10.5 will add full recursive import resolution.
    Ok(transforms)
}
