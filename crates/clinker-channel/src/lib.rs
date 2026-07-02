//! # Channel Authoring Guide
//!
//! Channels are per-tenant launch plans that bind a pipeline's or composition's
//! declared configuration and resources for a specific deployment context.
//! They enable multi-tenant ETL without duplicating pipeline definitions.
//!
//! ## What channels are for
//!
//! A single pipeline can serve multiple tenants (e.g., `acme_prod`,
//! `beta_staging`) by pairing it with different `.channel.yaml` files. Each
//! channel overrides only the declared config parameters — no mid-graph
//! patching, no internal node access.
//!
//! ## The `_channel:` header
//!
//! Every `.channel.yaml` file declares:
//!
//! - **`channel.name:`** — human-readable channel identifier.
//! - **`channel.target:`** — path to the target pipeline or composition
//!   (each channel file declares exactly one target).
//! - **`config.default:`** — values applied non-fixed on the channel's overlay
//!   layer. A later, higher-precedence layer may override them.
//! - **`config.fixed:`** — values applied with the `fixed` lock set on the same
//!   layer. A fixed value cannot be overridden by any higher-precedence layer.
//!
//! ## `fixed` vs `default` distinction
//!
//! `fixed` is not a separate layer — it is a lock flag on the value. A fixed
//! value is hard-pinned: no higher-precedence layer may change it. A default
//! value is a fallback that a higher layer may still override. Config resolves
//! through a fixed *semantic* layer order (never lexical or positional); higher
//! layers win unless a lower one is `fixed`:
//! `PipelineDefault < Group(s) by priority < ChannelWide < ChannelPerTarget`.
//!
//! ## Sealed composition internals
//!
//! Channels can only override parameters declared in the target's
//! `config_schema`. They cannot reach into sealed composition internals.
//! Attempting to bind an undeclared key produces error E105.
//!
//! ## DottedPath syntax
//!
//! Channel binding keys use dotted-path notation: `alias.param_name` for
//! composition config, or `param_name` alone for pipeline-level config.
//! See [`DottedPath`] for validation rules.
//!
//! ## Content-hash keying
//!
//! Channel files are content-hashed (BLAKE3) on load. Unchanged channels
//! produce identical hashes, enabling efficient cache invalidation —
//! recompilation is skipped when the channel content has not changed.

pub mod binding;
pub mod derivation;
pub mod discovery;
pub mod error;
pub mod group;
pub mod manifest;
pub mod overlay;
pub mod resolve;
pub mod selector;
pub mod staging_copy;

// Explicit re-exports at crate root.
pub use binding::{ChannelBinding, ChannelTarget, DottedPath, validate_channel_bindings};
pub use derivation::{
    GroupDerivation, GroupSelection, SelectionOutcome, apply_group_config, derive_groups,
    group_layer,
};
pub use discovery::{
    CHANNEL_MANIFEST_FILE, DiscoveredChannel, OverlayKind, ResolvedOverlay, channel_dir,
    channel_folder_path, resolve_channel_overlay, scan_channels, scan_groups,
};
pub use error::ChannelError;
pub use group::Group;
pub use manifest::{ChannelManifest, ChannelVars, ManifestHeader, OverlayFile, OverlayHeader};
pub use overlay::{ChannelOverlayResult, apply_channel_overlay};
pub use resolve::{
    AppliedGroup, GroupSource, InjectedNode, OverlayResolution, ResolveError, resolve,
};
pub use selector::{LabelSelector, SelectorError};
pub use staging_copy::{
    ReuseDecision, SourceStager, StagingError, StagingPlanEntry, open_source_file,
};
