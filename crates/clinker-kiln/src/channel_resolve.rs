//! Channel pipeline resolution — resolve a base pipeline through channel overrides.
//!
//! Bridges Kiln's pipeline signals with the `clinker-channel` crate's resolution
//! engine. Produces a `ChannelResolution` containing the resolved config, provenance
//! map, and tracking of which overrides were applied.

use std::path::{Path, PathBuf};

use clinker_channel::channel_override::{ChannelOverride, resolve_channel_with_inheritance};
use clinker_channel::composition::{ProvenanceMap, resolve_compositions};
use clinker_channel::error::ChannelError;
use clinker_channel::manifest::ChannelManifest;
use clinker_channel::workspace::{DefaultsConfig, WorkspaceConfig, WorkspaceRoot};
use clinker_core::config::{PipelineConfig, TransformEntry};

use crate::state::ChannelState;

/// Result of resolving a pipeline through a channel's overrides.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ChannelResolution {
    /// The fully resolved pipeline config (base + compositions + overrides).
    pub resolved_config: PipelineConfig,
    /// Provenance map: transform name -> source .comp.yaml path.
    pub provenance: ProvenanceMap,
    /// Which overrides were applied and how.
    pub overrides_applied: Vec<AppliedOverride>,
    /// Override source files that contributed to the resolution.
    pub override_source_files: Vec<PathBuf>,
}

/// A single override that was applied during resolution.
#[derive(Clone, Debug, PartialEq)]
pub struct AppliedOverride {
    /// Name of the affected transform/input/output.
    pub target_name: String,
    /// What kind of override was applied.
    pub kind: OverrideKind,
    /// Where the override came from.
    pub source: OverrideSource,
    /// Path to the override file.
    pub file: PathBuf,
}

/// The type of override applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OverrideKind {
    /// An existing transform/input/output was modified.
    Modified,
    /// A new transform was added.
    Added,
    /// A transform was removed.
    Removed,
}

/// Where an override originated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OverrideSource {
    /// From the channel's own .channel.yaml file.
    Channel,
    /// Inherited from a group.
    #[allow(dead_code)]
    Group(String),
}

/// Resolve a pipeline through a channel's overrides.
///
/// Steps:
/// 1. Load channel manifest and extract variables
/// 2. Clone the base pipeline and resolve compositions
/// 3. Apply group overrides (inheritance chain)
/// 4. Apply channel-specific override
/// 5. Track what changed for UI indicators
pub fn resolve_pipeline_for_channel(
    base_config: &PipelineConfig,
    channel_id: &str,
    channel_state: &ChannelState,
    pipeline_path: Option<&Path>,
) -> Result<ChannelResolution, ChannelError> {
    // Construct WorkspaceRoot from kiln config — no clinker.toml needed.
    let ws = WorkspaceRoot {
        root: channel_state.workspace_root.clone(),
        config: WorkspaceConfig {
            channels_dir: channel_state.channels_dir.clone(),
            groups_dir: channel_state.groups_dir.clone(),
            default_channel: channel_state.default_channel.clone(),
            pipeline_paths: Vec::new(),
        },
        defaults: DefaultsConfig::default(),
    };

    // Load channel manifest
    let channel_dir = ws.channel_dir(channel_id);
    let manifest = ChannelManifest::load(&channel_dir)?;
    let vars = manifest.vars_as_pairs();

    // Clone the base config for resolution
    let mut config = base_config.clone();

    // Snapshot transform names before resolution for diff tracking
    let base_transform_names: Vec<String> = config
        .transformations
        .iter()
        .filter_map(|e| match e {
            TransformEntry::Transform(t) => Some(t.name.clone()),
            _ => None,
        })
        .collect();

    // Step 1: Resolve compositions
    let mut provenance = resolve_compositions(&mut config, &ws, &vars)?;

    // Snapshot after composition resolution (before overrides)
    let pre_override_config = config.clone();

    // Step 2: Apply overrides with inheritance
    // Use a dummy pipeline path if none provided
    let dummy_path = PathBuf::from("pipeline.yaml");
    let pl_path = pipeline_path.unwrap_or(&dummy_path);

    config =
        resolve_channel_with_inheritance(config, channel_id, pl_path, &ws, &vars, &mut provenance)?;

    // Step 3: Track what changed for UI indicators
    let mut overrides_applied = Vec::new();
    let mut override_files = Vec::new();

    // Detect override files that exist
    let channel_override_path = ChannelOverride::path_for(pl_path, &channel_dir);
    if channel_override_path.exists() {
        override_files.push(channel_override_path.clone());
    }

    // Compare pre-override and post-override configs to find changes
    // Check transforms
    let post_transform_names: Vec<String> = config
        .transformations
        .iter()
        .filter_map(|e| match e {
            TransformEntry::Transform(t) => Some(t.name.clone()),
            _ => None,
        })
        .collect();

    // Detect added transforms
    for name in &post_transform_names {
        if !base_transform_names.contains(name) {
            overrides_applied.push(AppliedOverride {
                target_name: name.clone(),
                kind: OverrideKind::Added,
                source: OverrideSource::Channel,
                file: channel_override_path.clone(),
            });
        }
    }

    // Detect removed transforms
    for name in &base_transform_names {
        if !post_transform_names.contains(name) {
            overrides_applied.push(AppliedOverride {
                target_name: name.clone(),
                kind: OverrideKind::Removed,
                source: OverrideSource::Channel,
                file: channel_override_path.clone(),
            });
        }
    }

    // Detect modified transforms (CXL changed)
    for post_entry in &config.transformations {
        if let TransformEntry::Transform(post_t) = post_entry
            && let Some(pre_entry) = pre_override_config
                .transformations
                .iter()
                .find(|e| matches!(e, TransformEntry::Transform(t) if t.name == post_t.name))
            && let TransformEntry::Transform(pre_t) = pre_entry
            && (pre_t.cxl != post_t.cxl
                || pre_t.description != post_t.description
                || pre_t.local_window != post_t.local_window)
        {
            overrides_applied.push(AppliedOverride {
                target_name: post_t.name.clone(),
                kind: OverrideKind::Modified,
                source: OverrideSource::Channel,
                file: channel_override_path.clone(),
            });
        }
    }

    Ok(ChannelResolution {
        resolved_config: config,
        provenance,
        overrides_applied,
        override_source_files: override_files,
    })
}
