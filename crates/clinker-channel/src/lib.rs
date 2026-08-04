//! # Channel/Group Overlay System
//!
//! Channels and groups are the multi-tenant overlay layer over a base pipeline.
//! A single pipeline serves many tenants without duplication: each tenant is a
//! `channel/<id>/` folder whose manifest and per-target overlay files clobber
//! the base pipeline's declared config, `vars`, and structure.
//!
//! ## Layout
//!
//! - `channel/<id>/channel.cfg.yaml` — the per-channel [`manifest`]: identity
//!   `labels` (which drive group selectors), plus optional channel-wide
//!   `config` and `vars` that apply to every pipeline the channel runs.
//! - `channel/<id>/<target>.channel.yaml` — a per-target [`OverlayFile`]:
//!   `config` / `vars` clobber, an `overrides:` op stream, and `sources:`
//!   per-source config patches, all scoped to one pipeline or composition.
//! - `group/<name>.group.yaml` — a selector-scoped [`Group`]: a CXL boolean over
//!   channel `labels` picks the channels a group applies to, with a `priority`
//!   ordering matching groups.
//!
//! ## Layer stack
//!
//! Both application surfaces resolve through the fixed *semantic* order (never
//! lexical or positional), with clobber (full replace) between layers:
//!
//! ```text
//! PipelineDefault  <  Group(s) by priority  <  ChannelWide  <  ChannelPerTarget
//! ```
//!
//! A leaf whose `fixed: true` locks its value against every higher-precedence
//! layer.
//!
//! ## Two application surfaces
//!
//! - **Structural** — the `overrides:` op stream ([`OverlayOp`](clinker_plan::overlay_ops::OverlayOp))
//!   splices into the base AST *pre-compile* (via `CompileContext::overlay_ops`),
//!   so the effective DAG is what binds and lowers.
//! - **Value** — every `config:` candidate validates against a typed target
//!   plan before its winning value folds into executable compilation; the full
//!   provenance chain and validated runtime `vars` are applied post-compile.
//!
//! [`resolve`] ties discovery, group derivation, and both surfaces into one
//! [`OverlayResolution`] for a `(target, channel?, groups)` invocation; the CLI
//! (`run`, `explain`, `channels resolve`, `channels lint`) drives it.
//!
//! ## DottedPath syntax
//!
//! `config:` keys use dotted-path notation: `alias.param_name` for a
//! composition-node parameter, or `param_name` alone for pipeline-level config.
//! See [`DottedPath`] for validation rules.
//!
//! ## Content-hash keying
//!
//! A channel's overlay files are content-hashed (BLAKE3) into its
//! [`ChannelIdentity`](clinker_plan::plan::ChannelIdentity), so an unchanged
//! channel keeps a stable identity and a changed overlay invalidates any
//! identity-keyed cache.

pub mod derivation;
pub mod discovery;
pub mod dotted;
pub mod error;
pub mod group;
pub mod manifest;
pub mod overlay;
pub mod resolve;
pub mod selector;
pub mod staging_copy;

// Explicit re-exports at crate root.
pub use derivation::{
    GroupDerivation, GroupSelection, SelectionOutcome, apply_group_config, derive_groups,
    group_layer,
};
pub use discovery::{
    CHANNEL_MANIFEST_FILE, ChannelResource, ChannelTarget, DiscoveredChannel, OverlayKind,
    ResolvedOverlay, channel_dir, channel_folder_path, discover_channel_resource,
    resolve_channel_overlay, scan_channels, scan_groups,
};
pub use dotted::DottedPath;
pub use error::ChannelError;
pub use group::{Group, GroupTargetSet, ValidatedGroupTargets, validate_group_targets};
pub use manifest::{
    ChannelConfigValue, ChannelManifest, ChannelVarValue, ChannelVars, ManifestHeader,
    OverlayCandidate, OverlayFile, OverlayHeader, PipelineChannelFile,
};
pub use overlay::{ChannelOverlayResult, ResolvedChannelConfig};
pub use resolve::{
    AppliedGroup, GroupSource, InjectedNode, OverlayResolution, ResolveError, resolve,
    resolve_target_channel,
};
pub use selector::{LabelSelector, SelectorError};
pub use staging_copy::{
    ReuseDecision, SourceStager, StagingError, StagingPlanEntry, open_source_file,
};
