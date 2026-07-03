//! Channel manifest and per-target overlay serde types.
//!
//! These model the on-the-wire YAML of the channel-centric overlay layout:
//!
//! - [`ChannelManifest`] models `channel.cfg.yaml` — a per-channel manifest
//!   carrying identity `labels`, plus optional channel-wide `config`, `vars`,
//!   and `overrides` that apply to every pipeline this channel runs. The
//!   manifest is optional per channel: the containing folder name *is* the
//!   channel id, so a manifest is only needed when a channel has labels or
//!   channel-wide overlays.
//! - [`OverlayFile`] models a per-target overlay file — `<target>.channel.yaml`
//!   (pipeline overlay), `<target>.comp.yaml` (composition overlay), or a bare
//!   `<target>.yaml`. The `channel.target:` field is authoritative; the
//!   filename suffix is optional and secondary (it only aids reading a file
//!   out of context, e.g. in a diff).
//!
//! Both parse through `clinker_plan::yaml::from_str` (serde-saphyr, budgeted).
//! This module is parse-only: it defines the wire shapes and nothing else.
//! Key validation (dotted-path `config` keys, label scalar-ness), layer
//! resolution, and override application all live in later stages.

use std::path::{Path, PathBuf};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use clinker_plan::config::ScopedVarDecl;
use clinker_plan::config::SourceConfigPatch;
use clinker_plan::overlay_ops::OverlayOp;
use clinker_plan::yaml::Spanned;

use crate::error::ChannelError;

/// A parsed `channel.cfg.yaml` manifest.
///
/// ```yaml
/// channel:
///   name: globex
/// labels: { region: west, tier: enterprise }
/// config: { fraud_check.threshold: 0.9 }
/// fixed:  { fraud_check.mode: strict }   # locked against the per-target layer
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
    /// Channel-wide config clobber values, keyed by `alias.param` dotted
    /// path. Keys stay raw strings here; dotted-path validation is a later
    /// stage's concern. Applied non-fixed, so a higher-precedence layer may
    /// still override them.
    #[serde(default)]
    pub config: IndexMap<String, serde_json::Value>,
    /// Channel-wide **fixed** (locked) config values, same `alias.param`
    /// dotted-path grammar as [`Self::config`]. Applied with the layer `fixed`
    /// lock set: a fixed value at this `ChannelWide` layer cannot be overridden
    /// by any higher-precedence layer (the per-target overlay). For a key
    /// present in both maps, the `fixed` entry wins within the layer.
    #[serde(default)]
    pub fixed: IndexMap<String, serde_json::Value>,
    /// Channel-wide var overlays, using the same four scopes a pipeline's
    /// `vars:` block uses.
    #[serde(default)]
    pub vars: ChannelVars,
    /// Channel-wide ordered override op list, applied at the `ChannelWide`
    /// layer. Each op keeps its source [`Spanned`] location so a later
    /// ill-typed-op diagnostic anchors to the offending op rather than the
    /// base pipeline.
    #[serde(default)]
    pub overrides: Vec<Spanned<OverlayOp>>,
}

/// The `channel:` header of a manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestHeader {
    /// Human-readable channel identifier.
    pub name: String,
}

/// A parsed per-target overlay file (`<target>.channel.yaml` /
/// `<target>.comp.yaml` / bare `<target>.yaml`).
///
/// ```yaml
/// channel:
///   target: ../../pipeline/order_fulfillment.yaml
/// config: { fraud_check.threshold: 0.95 }
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
    /// Applied non-fixed at the highest `ChannelPerTarget` layer.
    #[serde(default)]
    pub config: IndexMap<String, serde_json::Value>,
    /// Per-target **fixed** (locked) config values, same `alias.param`
    /// dotted-path grammar as [`Self::config`]. Applied with the layer `fixed`
    /// lock set at the `ChannelPerTarget` layer. For a key present in both
    /// maps, the `fixed` entry wins within the layer.
    #[serde(default)]
    pub fixed: IndexMap<String, serde_json::Value>,
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
    /// column ops (`schema`), nested-array explosion/join (`array_paths`), and
    /// scalar per-format input `options`. Scoped to this one target, so
    /// source-node names resolve unambiguously against the overlaid pipeline.
    #[serde(default)]
    pub sources: IndexMap<String, SourceConfigPatch>,
}

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
/// [`ScopedVarDecl`] (`{ type, default }`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelVars {
    /// `$vars.*` static-config overlays, keyed by var name.
    #[serde(default, rename = "static")]
    pub static_scope: IndexMap<String, ScopedVarDecl>,
    /// `$pipeline.*` overlays, keyed by var name.
    #[serde(default)]
    pub pipeline: IndexMap<String, ScopedVarDecl>,
    /// `$source.<src>.*` overlays: outer key is the source-node name, inner
    /// key is the var name.
    #[serde(default)]
    pub source: IndexMap<String, IndexMap<String, ScopedVarDecl>>,
    /// `$record.*` overlays, keyed by var name.
    #[serde(default)]
    pub record: IndexMap<String, ScopedVarDecl>,
}

impl ChannelManifest {
    /// Parse a `channel.cfg.yaml` manifest from raw bytes. `source_path` is
    /// used only for diagnostic context.
    pub fn from_yaml_bytes(bytes: &[u8], source_path: PathBuf) -> Result<Self, ChannelError> {
        parse_yaml(bytes, source_path)
    }

    /// Load and parse a `channel.cfg.yaml` manifest from disk.
    pub fn load(path: &Path) -> Result<Self, ChannelError> {
        let bytes = std::fs::read(path)?;
        Self::from_yaml_bytes(&bytes, path.to_path_buf())
    }
}

impl OverlayFile {
    /// Parse a per-target overlay file from raw bytes. `source_path` is used
    /// only for diagnostic context — the overlay `target` comes from the YAML
    /// body, never from the filename.
    pub fn from_yaml_bytes(bytes: &[u8], source_path: PathBuf) -> Result<Self, ChannelError> {
        parse_yaml(bytes, source_path)
    }

    /// Load and parse a per-target overlay file from disk.
    pub fn load(path: &Path) -> Result<Self, ChannelError> {
        let bytes = std::fs::read(path)?;
        Self::from_yaml_bytes(&bytes, path.to_path_buf())
    }
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
    clinker_plan::yaml::from_str(text).map_err(|e| ChannelError::Yaml {
        path: source_path,
        source: Box::new(e.0),
    })
}
