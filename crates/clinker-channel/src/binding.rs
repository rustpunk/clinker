//! Channel binding types and YAML parser.
//!
//! A `.channel.yaml` file declares a named channel that targets a pipeline
//! or composition and overlays config/resource values onto its parameters.
//! Parsing computes a BLAKE3 content hash from the raw file bytes for
//! cache-invalidation identity.

use std::path::{Path, PathBuf};

use indexmap::IndexMap;
use serde::Deserialize;

use clinker_core::config::composition::CompositionSymbolTable;
use clinker_core::error::{Diagnostic, LabeledSpan};
use clinker_core::span::Span;

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
/// and `LayerKind::ChannelFixed` respectively; channel-fixed layers merge with
/// deterministic precedence over variable (default) layers. Resources follow
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
    /// BLAKE3 hash of the raw file bytes, used as cache-invalidation identity.
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

// ── Channel validation ─────────────────────────────────────────────────

/// Maximum number of `.channel.yaml` files per workspace scan.
const WORKSPACE_CHANNEL_BUDGET: usize = 50;

/// Maximum filesystem depth for channel workspace walks.
const WORKSPACE_CHANNEL_MAX_DEPTH: usize = 16;

/// Scan a workspace root for `.channel.yaml` files and parse each into a
/// [`ChannelBinding`].
///
/// Companion to `scan_workspace_signatures` in clinker-core — uses the
/// same walkdir + budget pattern. Symlinks rejected, depth bounded.
pub fn scan_workspace_channels(
    workspace_root: &Path,
) -> Result<Vec<ChannelBinding>, Vec<Diagnostic>> {
    use walkdir::WalkDir;

    if !workspace_root.exists() {
        return Ok(Vec::new());
    }

    let mut bindings = Vec::new();
    let mut diagnostics: Vec<Diagnostic> = Vec::new();

    let walker = WalkDir::new(workspace_root)
        .follow_links(false)
        .max_depth(WORKSPACE_CHANNEL_MAX_DEPTH)
        .into_iter();

    for entry in walker {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };

        if !entry.file_type().is_file() {
            continue;
        }

        let path = entry.path();
        if !is_channel_yaml(path) {
            continue;
        }

        if bindings.len() >= WORKSPACE_CHANNEL_BUDGET {
            diagnostics.push(Diagnostic::error(
                "E101",
                format!(
                    "channel file budget exceeded: more than {WORKSPACE_CHANNEL_BUDGET} \
                     .channel.yaml files in workspace"
                ),
                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
            ));
            return Err(diagnostics);
        }

        match ChannelBinding::load(path) {
            Ok(binding) => bindings.push(binding),
            Err(e) => {
                diagnostics.push(Diagnostic::error(
                    "E101",
                    format!("failed to parse {}: {e}", path.display()),
                    LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                ));
                return Err(diagnostics);
            }
        }
    }

    Ok(bindings)
}

/// Validate channel bindings against the composition symbol table.
///
/// For composition targets: verifies the target exists in the symbol table
/// (E103) and that all config keys reference declared params (E105).
/// Pipeline targets: only validates that the target path exists on disk.
pub fn validate_channel_bindings(
    bindings: &[ChannelBinding],
    symbol_table: &CompositionSymbolTable,
    workspace_root: &Path,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();

    for binding in bindings {
        match &binding.target {
            ChannelTarget::Composition(comp_path) => {
                // Resolve the workspace-relative key used in the symbol table.
                let key = normalize_target_path(comp_path, workspace_root);

                match symbol_table.get(&key) {
                    None => {
                        diagnostics.push(Diagnostic::error(
                            "E103",
                            format!(
                                "channel {:?} targets unknown composition: {}",
                                binding.name,
                                comp_path.display()
                            ),
                            LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                        ));
                    }
                    Some(signature) => {
                        // Validate config keys against declared config_schema.
                        validate_config_keys(
                            &binding.config_default,
                            signature,
                            &binding.name,
                            &mut diagnostics,
                        );
                        validate_config_keys(
                            &binding.config_fixed,
                            signature,
                            &binding.name,
                            &mut diagnostics,
                        );
                    }
                }
            }
            ChannelTarget::Pipeline(pipeline_path) => {
                // Pipeline-target: validate path exists, skip config key
                // validation (deferred per V-2-2).
                let resolved = if pipeline_path.is_relative() {
                    workspace_root.join(pipeline_path)
                } else {
                    pipeline_path.clone()
                };
                if !resolved.exists() {
                    diagnostics.push(Diagnostic::error(
                        "E103",
                        format!(
                            "channel {:?} targets pipeline that does not exist: {}",
                            binding.name,
                            pipeline_path.display()
                        ),
                        LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                    ));
                }
            }
        }
    }

    diagnostics
}

/// Validate config keys from a channel binding against a composition's
/// declared `config_schema`. Emits E105 for undeclared keys.
fn validate_config_keys(
    config: &IndexMap<DottedPath, serde_json::Value>,
    signature: &clinker_core::config::composition::CompositionSignature,
    channel_name: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    for dotted_path in config.keys() {
        // For composition targets, the dotted path is just the param name
        // (single segment) since the channel targets the composition
        // directly. Two-segment paths (alias.param) are for pipeline-level
        // channels that target specific compositions within a pipeline.
        let param_name = match dotted_path.segments() {
            (None, param) => param,
            (Some(_alias), param) => param,
        };

        if !signature.config_schema.contains_key(param_name) {
            diagnostics.push(Diagnostic::error(
                "E105",
                format!(
                    "channel {:?}: config key {:?} is not declared in composition \
                     {:?} config_schema",
                    channel_name,
                    dotted_path.as_str(),
                    signature.name,
                ),
                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
            ));
        }
    }
}

fn is_channel_yaml(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.ends_with(".channel.yaml"))
        .unwrap_or(false)
}

/// Normalize a target path for lookup in the composition symbol table.
///
/// The symbol table keys are workspace-relative paths. Channel target paths
/// are typically `./compositions/foo.comp.yaml` — strip the leading `./`
/// to match the symbol table key format.
fn normalize_target_path(target: &Path, _workspace_root: &Path) -> PathBuf {
    let s = target.to_string_lossy();
    if let Some(stripped) = s.strip_prefix("./") {
        PathBuf::from(stripped)
    } else {
        target.to_path_buf()
    }
}
