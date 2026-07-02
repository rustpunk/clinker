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

use crate::error::ChannelError;
use crate::manifest::ChannelVars;

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
    /// Value-clobber config surface. Keys are `alias.param` dotted paths (kept
    /// as raw strings here; dotted-path validation happens at apply time).
    pub config: IndexMap<String, serde_json::Value>,
    /// Value-clobber vars surface, in the same four scopes a pipeline's `vars:`
    /// block uses (`static` / `pipeline` / `source` / `record`). Reuses the
    /// channel overlay's [`ChannelVars`](crate::manifest::ChannelVars) type.
    pub vars: ChannelVars,
    /// Ordered override ops, preserved verbatim. The typed op vocabulary is
    /// owned by the overlay op engine and interpreted there; this stage only
    /// captures the raw entries without losing information.
    pub overrides: Vec<serde_json::Value>,
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

        Ok(Group {
            name: raw.group.name,
            selector: raw.group.selector,
            priority: raw.group.priority,
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
    config: IndexMap<String, serde_json::Value>,
    #[serde(default)]
    vars: ChannelVars,
    #[serde(default)]
    overrides: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGroupMeta {
    name: String,
    #[serde(default, rename = "match")]
    selector: Option<String>,
    #[serde(default = "default_priority")]
    priority: i64,
}
