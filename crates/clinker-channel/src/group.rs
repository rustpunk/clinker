//! Group serde model (`group/*.group.yaml`).
//!
//! A group is a reusable overlay layer that sits between the pipeline default
//! and the channel layers in the fixed precedence order
//! `pipeline-default < group(s) by priority < channel-wide < channel-per-target`.
//! It carries the same two overlay surfaces every layer carries — a value
//! clobber (`config:` / `vars:`) and an ordered op list (`overrides:`).
//!
//! A group plays two roles under one concept:
//!
//! - **Selector-derived grouping** — when `match:` is present, the group is
//!   selected automatically for every channel whose `labels` satisfy the CXL
//!   boolean. Multiple matching groups are ordered by `priority` (higher wins).
//! - **Explicit-only profile/variant** — when `match:` is absent, the group is
//!   never auto-selected; it applies only when invoked by name (`--group`).
//!
//! Groups are channel-agnostic: their overrides never read channel labels, so a
//! group can run standalone.
//!
//! This module is **parse only**. The `match:` selector is a CXL boolean string
//! here; compiling and evaluating it against a channel's labels happens later.
//! The `overrides:` entries are preserved verbatim as raw values; the typed op
//! vocabulary (`add` / `remove` / `replace` / `set` / `patch_schema` / `bypass`)
//! is owned by the overlay op engine and interpreted there, not here.
//!
//! The four-scope `vars:` surface reuses [`ChannelVars`](crate::manifest::ChannelVars),
//! the same value-clobber vars type the channel manifest and per-target overlay
//! files use — a group's vars are identical in shape to a channel's.

use std::path::{Path, PathBuf};

use indexmap::IndexMap;
use serde::Deserialize;

use clinker_plan::overlay_ops::OverlayOp;
use clinker_plan::resources::{CatalogResourceKind, LogicalResourceId, WorkspaceCatalog};
use clinker_plan::yaml::Spanned;

use crate::error::ChannelError;
use crate::manifest::{ChannelConfigValue, ChannelVars};

/// Priority applied to a group that omits `priority:`.
///
/// Higher priority wins among multiple matching groups, so the baseline is the
/// lowest rung: an unprioritized group is overridden by any group that opts
/// into an explicit positive priority.
const DEFAULT_GROUP_PRIORITY: i64 = 0;

fn default_priority() -> i64 {
    DEFAULT_GROUP_PRIORITY
}

/// A parsed `group/*.group.yaml` file.
///
/// The fields mirror the canonical group schema. `config` / `vars` are the
/// value-clobber surface; `overrides` is the ordered op-list surface preserved
/// verbatim for the op engine to interpret.
#[derive(Debug, Clone)]
pub struct Group {
    /// Group identifier (`group.name`). Also the handle used by `--group`.
    pub name: String,
    /// Optional CXL boolean over channel `labels` (YAML `group.match`). `None`
    /// means the group is explicit-only (never auto-selected). The expression
    /// is stored as text here and compiled later.
    pub selector: Option<String>,
    /// Selection priority (`group.priority`); higher wins among multiple
    /// matching groups. Defaults to [`DEFAULT_GROUP_PRIORITY`] when absent.
    pub priority: i64,
    /// Catalog pipeline/composition identities this group is allowed to affect.
    pub targets: GroupTargetSet,
    /// Value-clobber config surface. Keys are `alias.param` dotted paths (kept
    /// as raw strings here; dotted-path validation happens at apply time).
    /// Each leaf independently carries its `fixed` lock.
    pub config: IndexMap<String, ChannelConfigValue>,
    /// Value-clobber vars surface, in the same four scopes a pipeline's `vars:`
    /// block uses (`static` / `pipeline` / `source` / `record`). Reuses the
    /// channel overlay's [`ChannelVars`](crate::manifest::ChannelVars) type.
    pub vars: ChannelVars,
    /// Ordered override ops applied at this group's `Group` layer. Each op
    /// keeps its source [`Spanned`] location so a later ill-typed-op
    /// diagnostic anchors to the offending op rather than the base pipeline.
    pub overrides: Vec<Spanned<OverlayOp>>,
}

impl Group {
    /// Parse a `group/*.group.yaml` from raw bytes.
    ///
    /// `source_path` is used only for diagnostic context on parse failure. All
    /// parsing goes through [`clinker_plan::yaml::from_str`] so the shared
    /// parse budget applies.
    pub fn from_yaml_bytes(bytes: &[u8], source_path: PathBuf) -> Result<Self, ChannelError> {
        let text = std::str::from_utf8(bytes).map_err(|e| ChannelError::Utf8 {
            path: source_path.clone(),
            source: e,
        })?;
        let raw: RawGroupFile =
            clinker_plan::yaml::from_str(text).map_err(|e| ChannelError::Yaml {
                path: source_path,
                source: Box::new(e.0),
            })?;

        if raw.group.targets.pipelines.is_empty() && raw.group.targets.compositions.is_empty() {
            return Err(ChannelError::InvalidGroup {
                group: raw.group.name,
                line: 1,
                reason: "`group.targets` must not be empty".to_string(),
                correction: "use `targets: { pipelines: [sales.orders] }` or list compositions"
                    .to_string(),
            });
        }

        Ok(Group {
            name: raw.group.name,
            selector: raw.group.selector,
            priority: raw.group.priority,
            targets: raw.group.targets,
            config: raw.config,
            vars: raw.vars,
            overrides: raw.overrides,
        })
    }

    /// Load and parse a single `group/*.group.yaml` file from disk.
    pub fn load(path: &Path) -> Result<Self, ChannelError> {
        let bytes = std::fs::read(path)?;
        Self::from_yaml_bytes(&bytes, path.to_path_buf())
    }
}

// ── Serde intermediate types ────────────────────────────────────────────

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGroupFile {
    group: RawGroupMeta,
    #[serde(default)]
    config: IndexMap<String, ChannelConfigValue>,
    #[serde(default)]
    vars: ChannelVars,
    #[serde(default)]
    overrides: Vec<Spanned<OverlayOp>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGroupMeta {
    name: String,
    #[serde(default, rename = "match")]
    selector: Option<String>,
    #[serde(default = "default_priority")]
    priority: i64,
    #[serde(default)]
    targets: GroupTargetSet,
}

/// Explicit target scope for a group. Selectors may only narrow this set.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupTargetSet {
    /// Catalog pipeline identities this group may affect.
    #[serde(default)]
    pub pipelines: Vec<Spanned<String>>,
    /// Catalog composition identities whose owning closures this group may affect.
    #[serde(default)]
    pub compositions: Vec<Spanned<String>>,
}

/// Catalog-validated group target identities.
#[derive(Debug, Clone, Default)]
pub struct ValidatedGroupTargets {
    pipelines: std::collections::BTreeSet<LogicalResourceId>,
    compositions: std::collections::BTreeMap<LogicalResourceId, PathBuf>,
}

impl ValidatedGroupTargets {
    /// Whether this target's pipeline or composition closure intersects the
    /// group's catalog-validated target set.
    pub fn admits(&self, target: &crate::discovery::ChannelTarget) -> bool {
        self.pipelines.contains(&target.pipeline)
            || target.composition_paths.iter().any(|path| {
                self.compositions
                    .values()
                    .any(|candidate| candidate == path)
            })
    }
}

/// Resolve every group target through the workspace catalog before selection.
pub fn validate_group_targets(
    catalog: &WorkspaceCatalog,
    group: &Group,
) -> Result<ValidatedGroupTargets, ChannelError> {
    if group.targets.pipelines.is_empty() && group.targets.compositions.is_empty() {
        return Err(ChannelError::InvalidGroupTargets {
            group: group.name.clone(),
            reason: "`group.targets` must list at least one pipeline or composition; for example `targets: { pipelines: [sales.orders] }`".to_string(),
        });
    }

    let mut validated = ValidatedGroupTargets::default();
    for target in &group.targets.pipelines {
        let id = LogicalResourceId::parse(&target.value).map_err(|error| {
            ChannelError::InvalidGroupTargets {
                group: group.name.clone(),
                reason: error.to_string(),
            }
        })?;
        catalog
            .resolve(CatalogResourceKind::Pipeline, &id)
            .map_err(|error| ChannelError::InvalidGroupTargets {
                group: group.name.clone(),
                reason: error.to_string(),
            })?;
        validated.pipelines.insert(id);
    }
    for target in &group.targets.compositions {
        let id = LogicalResourceId::parse(&target.value).map_err(|error| {
            ChannelError::InvalidGroupTargets {
                group: group.name.clone(),
                reason: error.to_string(),
            }
        })?;
        let path = catalog
            .resolve(CatalogResourceKind::Composition, &id)
            .map_err(|error| ChannelError::InvalidGroupTargets {
                group: group.name.clone(),
                reason: error.to_string(),
            })?;
        validated.compositions.insert(id, path.to_path_buf());
    }
    Ok(validated)
}
