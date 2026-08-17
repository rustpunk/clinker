//! Channel manifest and per-target overlay serde types.
//!
//! These model the on-the-wire YAML of the channel-centric overlay layout:
//!
//! - [`ChannelManifest`] models `channel.cfg.yaml` — a per-channel manifest
//!   carrying the catalog channel identity, explicit pipeline targets,
//!   identity `labels`, and optional channel-wide `config` and `vars`.
//! - [`OverlayFile`] models one pipeline-specific file. Its authoritative
//!   `channel.target:` is a logical catalog pipeline identity; filenames and
//!   basenames do not establish target identity.
//!
//! Both parse through `clinker_plan::yaml::from_str` (serde-saphyr, budgeted).
//! This module is parse-only: it defines the wire shapes and nothing else.
//! Key validation (dotted-path `config` keys, label scalar-ness), layer
//! resolution, and override application all live in later stages.

use std::path::{Path, PathBuf};

use indexmap::IndexMap;
use serde::{Deserialize, Deserializer, Serialize};

use clinker_plan::config::ScopedVarType;
use clinker_plan::config::SourceConfigPatch;
use clinker_plan::config::is_reserved_credential_selector;
use clinker_plan::overlay_ops::OverlayOp;
use clinker_plan::resources::LogicalResourceId;
use clinker_plan::yaml::{Location, Spanned};

use crate::error::ChannelError;

/// A parsed `channel.cfg.yaml` manifest.
///
/// ```yaml
/// channel:
///   name: globex
///   targets: [sales.orders]
/// labels: { region: west, tier: enterprise }
/// config:
///   fraud_check.threshold: { value: 0.9 }
///   fraud_check.mode: { value: strict, fixed: true }
/// vars:
///   static: { currency: { type: string, default: "USD" } }
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelManifest {
    /// The manifest header — carries the channel `name`.
    pub channel: ManifestHeader,
    /// Channel identity labels, order-preserving. Labels drive group
    /// selectors; they are identity, never a pipeline override. Values are
    /// kept opaque here — scalar-ness is enforced by a later stage.
    #[serde(default)]
    pub labels: IndexMap<String, serde_json::Value>,
    /// Channel-wide config candidates, each written as `{ value, fixed }`.
    #[serde(default)]
    pub config: IndexMap<String, ChannelConfigValue>,
    /// Channel-wide typed resource clobbers keyed `composition-node.slot`.
    #[serde(default)]
    pub resources: IndexMap<String, ResourceOverlayValue>,
    /// Channel-wide var overlays, using the same four scopes a pipeline's
    /// `vars:` block uses.
    #[serde(default)]
    pub vars: ChannelVars,
}

/// The `channel:` header of a manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestHeader {
    /// Human-readable channel identifier.
    pub name: String,
    /// Catalog pipeline identities this channel is allowed to run.
    #[serde(default)]
    pub targets: Vec<Spanned<String>>,
}

/// One authored overlay leaf with its value, fixed bit, and exact YAML spans.
#[derive(Debug, Clone)]
pub struct OverlayCandidate<T> {
    /// Authored candidate value.
    pub value: T,
    /// Whether this candidate locks the key against higher-precedence layers.
    pub fixed: bool,
    /// Exact YAML location of `value`.
    pub value_span: Location,
    /// Exact YAML location of `fixed`, when authored.
    pub fixed_span: Option<Location>,
}

/// One channel config candidate with its value, lock, and authored spans.
pub type ChannelConfigValue = OverlayCandidate<serde_json::Value>;
/// One typed resource candidate. The value is only a logical catalog id.
pub type ResourceOverlayValue = OverlayCandidate<LogicalResourceId>;

impl<'de, T> Deserialize<'de> for OverlayCandidate<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RawCandidate<T> {
            value: Spanned<T>,
            #[serde(default)]
            fixed: Option<Spanned<bool>>,
        }

        let raw = RawCandidate::<T>::deserialize(deserializer)?;
        Ok(Self {
            value: raw.value.value,
            fixed: raw.fixed.as_ref().is_some_and(|fixed| fixed.value),
            value_span: raw.value.referenced,
            fixed_span: raw.fixed.map(|fixed| fixed.referenced),
        })
    }
}

/// A parsed per-target overlay file (`<target>.channel.yaml` /
/// `<target>.comp.yaml` / bare `<target>.yaml`).
///
/// ```yaml
/// channel:
///   target: sales.orders
/// config: { fraud_check.threshold: { value: 0.95 } }
/// vars:   { static: { currency: { type: string, default: "USD" } } }
/// overrides: [ ... ]
/// sources:
///   orders: { schema: { amount: { type: float } }, options: { delimiter: "|" } }
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OverlayFile {
    /// The overlay header — carries the authoritative `target`.
    pub channel: OverlayHeader,
    /// Per-target config clobber values, keyed by `alias.param` dotted path.
    /// Each leaf independently carries its `fixed` lock at the highest layer.
    #[serde(default)]
    pub config: IndexMap<String, ChannelConfigValue>,
    /// Per-target typed resource clobbers keyed `composition-node.slot`.
    #[serde(default)]
    pub resources: IndexMap<String, ResourceOverlayValue>,
    /// Per-target var overlays, using the same four scopes a pipeline's
    /// `vars:` block uses.
    #[serde(default)]
    pub vars: ChannelVars,
    /// Per-target ordered override op list, applied at the highest
    /// `ChannelPerTarget` layer. Each op keeps its source [`Spanned`]
    /// location — see [`ChannelManifest::overrides`].
    #[serde(default)]
    pub overrides: Vec<Spanned<OverlayOp>>,
    /// Per-source config patches, keyed by source-node name. Applied to the
    /// parsed pipeline config before validation/compile (via
    /// [`apply_source_patches`](clinker_plan::config::apply_source_patches)), so
    /// the run behaves as if the source YAML had been hand-edited: CXL-typed
    /// column ops (`schema`), multi-value fan-out and in-cell parsing
    /// (`split_to_rows` / `split_values`),
    /// scalar per-format input `options`, X12 nested-envelope declarations
    /// (`group_section` / `set_section`), HL7 composite-field splits
    /// (`split_fields`), and multi-record flat-file record types
    /// (`records` / `discriminator`). Scoped to this one target, so source-node
    /// names resolve unambiguously against the overlaid pipeline.
    #[serde(default)]
    pub sources: IndexMap<String, SourceConfigPatch>,
}

/// Target-specific channel document selected by logical pipeline identity.
pub type PipelineChannelFile = OverlayFile;

/// The `channel:` header of an overlay file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OverlayHeader {
    /// Path to the overlaid pipeline or composition. Authoritative: the
    /// parsed value comes from the YAML, independent of the enclosing
    /// filename. The filename suffix (`.channel.yaml` / `.comp.yaml` /
    /// bare `.yaml`) is optional and, when present, must agree.
    pub target: String,
}

/// Var overlays, mirroring the four scopes a pipeline's `vars:` block uses
/// (`$vars.*` / `$pipeline.*` / `$source.*` / `$record.*`). Each leaf is a
/// [`ChannelVarValue`] (`{ type, default, fixed }`).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelVars {
    /// `$vars.*` static-config overlays, keyed by var name.
    #[serde(default, rename = "static")]
    pub static_scope: IndexMap<String, ChannelVarValue>,
    /// `$pipeline.*` overlays, keyed by var name.
    #[serde(default)]
    pub pipeline: IndexMap<String, ChannelVarValue>,
    /// `$source.<src>.*` overlays: outer key is the source-node name, inner
    /// key is the var name.
    #[serde(default)]
    pub source: IndexMap<String, IndexMap<String, ChannelVarValue>>,
    /// `$record.*` overlays, keyed by var name.
    #[serde(default)]
    pub record: IndexMap<String, ChannelVarValue>,
}

/// A channel variable declaration with the public leaf-level fixed bit.
#[derive(Debug, Clone)]
pub struct ChannelVarValue {
    /// Declared type of the variable candidate.
    pub var_type: ScopedVarType,
    /// Authored default/override value, when present.
    pub default: Option<serde_json::Value>,
    /// Whether this candidate locks the variable against higher layers.
    pub fixed: bool,
    /// Exact YAML location of `type`.
    pub type_span: Location,
    /// Exact YAML location of `default`, when authored.
    pub default_span: Option<Location>,
    /// Exact YAML location of `fixed`, when authored.
    pub fixed_span: Option<Location>,
}

impl<'de> Deserialize<'de> for ChannelVarValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RawVar {
            #[serde(rename = "type")]
            var_type: Spanned<ScopedVarType>,
            #[serde(default)]
            default: Option<Spanned<serde_json::Value>>,
            #[serde(default)]
            fixed: Option<Spanned<bool>>,
        }

        let raw = RawVar::deserialize(deserializer)?;
        Ok(Self {
            var_type: raw.var_type.value,
            default: raw.default.as_ref().map(|value| value.value.clone()),
            fixed: raw.fixed.as_ref().is_some_and(|fixed| fixed.value),
            type_span: raw.var_type.referenced,
            default_span: raw.default.map(|value| value.referenced),
            fixed_span: raw.fixed.map(|fixed| fixed.referenced),
        })
    }
}

impl ChannelManifest {
    /// Parse a `channel.cfg.yaml` manifest from raw bytes. `source_path` is
    /// used only for diagnostic context.
    pub fn from_yaml_bytes(bytes: &[u8], source_path: PathBuf) -> Result<Self, ChannelError> {
        let manifest: Self = parse_yaml(bytes, source_path).map_err(rewrite_manifest_error)?;
        if manifest.channel.targets.is_empty() {
            return Err(ChannelError::InvalidManifest {
                line: 1,
                reason: "`channel.targets` must contain at least one catalog pipeline identity"
                    .to_string(),
                correction: "use `channel: { name: tenant.acme, targets: [sales.orders] }`"
                    .to_string(),
            });
        }
        validate_resource_overlay_keys(&manifest.resources)?;
        Ok(manifest)
    }

    /// Load and parse a `channel.cfg.yaml` manifest from disk.
    pub fn load(path: &Path) -> Result<Self, ChannelError> {
        let bytes = std::fs::read(path)?;
        Self::from_yaml_bytes(&bytes, path.to_path_buf())
    }
}

fn rewrite_manifest_error(error: ChannelError) -> ChannelError {
    let message = error.to_string();
    if message.contains("unknown field `fixed`") || message.contains("expected mapping start") {
        ChannelError::InvalidManifest {
            line: 1,
            reason:
                "channel-wide config entries must use the leaf form; `fixed` is not a sibling block"
                    .to_string(),
            correction: "write each entry as `config: { key: { value: VALUE, fixed: true } }`"
                .to_string(),
        }
    } else if message.contains("unknown field `overrides`")
        || message.contains("unknown field `sources`")
    {
        let field = if message.contains("unknown field `overrides`") {
            "overrides"
        } else {
            "sources"
        };
        ChannelError::InvalidManifest {
            line: 1,
            reason: format!(
                "channel-wide field `{field}` is forbidden; manifests may declare only labels, config, and vars"
            ),
            correction:
                "move structural, source, and schema operations into a declared target file"
                    .to_string(),
        }
    } else {
        error
    }
}

impl OverlayFile {
    /// Parse a per-target overlay file from raw bytes. `source_path` is used
    /// only for diagnostic context — the overlay `target` comes from the YAML
    /// body, never from the filename.
    pub fn from_yaml_bytes(bytes: &[u8], source_path: PathBuf) -> Result<Self, ChannelError> {
        let overlay: Self = parse_yaml(bytes, source_path)?;
        validate_resource_overlay_keys(&overlay.resources)?;
        Ok(overlay)
    }

    /// Load and parse a per-target overlay file from disk.
    pub fn load(path: &Path) -> Result<Self, ChannelError> {
        let bytes = std::fs::read(path)?;
        Self::from_yaml_bytes(&bytes, path.to_path_buf())
    }
}

pub(crate) fn validate_resource_overlay_keys(
    resources: &IndexMap<String, ResourceOverlayValue>,
) -> Result<(), ChannelError> {
    for (address, candidate) in resources {
        let mut segments = address.split('.');
        let node = segments.next().unwrap_or_default();
        let slot = segments.next().unwrap_or_default();
        if node.is_empty() || slot.is_empty() || segments.next().is_some() {
            return Err(ChannelError::InvalidManifest {
                line: candidate.value_span.line(),
                reason: format!(
                    "resource key `{address}` is malformed; resource overlays address one public composition slot"
                ),
                correction:
                    "use `resources: { composition_node.slot: { value: catalog.resource } }`"
                        .to_string(),
            });
        }
        if is_reserved_credential_selector(slot) {
            return Err(ChannelError::InvalidManifest {
                line: candidate.value_span.line(),
                reason: format!(
                    "resource key `{address}` attempts to select credentials from an overlay"
                ),
                correction: "remove the credential/profile key; credential selection is an explicit run preflight choice"
                    .to_string(),
            });
        }
    }
    Ok(())
}

/// Shared parse path for both file kinds: UTF-8 check, then the canonical
/// budgeted YAML chokepoint.
fn parse_yaml<T>(bytes: &[u8], source_path: PathBuf) -> Result<T, ChannelError>
where
    T: for<'de> Deserialize<'de>,
{
    let text = std::str::from_utf8(bytes).map_err(|e| ChannelError::Utf8 {
        path: source_path.clone(),
        source: e,
    })?;
    validate_resource_overlay_shape(text, &source_path)?;
    clinker_plan::yaml::from_str(text).map_err(|e| ChannelError::Yaml {
        path: source_path,
        source: Box::new(e.0),
    })
}

/// Reject non-scalar resource candidates before typed deserialization can
/// render their authored payload in a YAML type-error snippet.
///
/// The temporary document is bounded by the canonical YAML parser budget and
/// is dropped before the typed manifest is parsed.
pub(crate) fn validate_resource_overlay_shape(
    text: &str,
    source_path: &Path,
) -> Result<(), ChannelError> {
    let document: serde_json::Value =
        clinker_plan::yaml::from_str(text).map_err(|error| ChannelError::Yaml {
            path: source_path.to_path_buf(),
            source: Box::new(error.0),
        })?;
    let Some(resources) = document.as_object().and_then(|root| root.get("resources")) else {
        return Ok(());
    };
    let Some(resources) = resources.as_object() else {
        return Err(invalid_resource_overlay_shape());
    };
    for candidate in resources.values() {
        let Some(candidate) = candidate.as_object() else {
            return Err(invalid_resource_overlay_shape());
        };
        let value = candidate.get("value").and_then(serde_json::Value::as_str);
        if candidate.len() > 2
            || candidate.keys().any(|key| key != "value" && key != "fixed")
            || value.is_none()
            || candidate
                .get("fixed")
                .is_some_and(|fixed| !fixed.is_boolean())
            || value.is_some_and(|value| LogicalResourceId::parse(value).is_err())
        {
            return Err(invalid_resource_overlay_shape());
        }
    }
    Ok(())
}

fn invalid_resource_overlay_shape() -> ChannelError {
    ChannelError::InvalidManifest {
        line: 1,
        reason: "resource overlay values must be secret-free logical catalog identities"
            .to_string(),
        correction:
            "use `resources: { composition_node.slot: { value: catalog.resource, fixed: false } }`"
                .to_string(),
    }
}
