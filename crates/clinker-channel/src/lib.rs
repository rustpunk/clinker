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
//! - **`config.default:`** — values applied as `ChannelDefault` provenance
//!   layer. These can be overridden by `ChannelFixed` or `InspectorEdit`.
//! - **`config.fixed:`** — values applied as `ChannelFixed` provenance layer.
//!   These win over all other layers and cannot be overridden.
//!
//! ## `fixed` vs `default` distinction
//!
//! `ChannelFixed` is the highest-priority override — it represents a
//! hard-pinned value that the channel author does not want changed.
//! `ChannelDefault` provides a fallback that can still be overridden by
//! higher-priority layers. The provenance chain is bounded at 4 layers,
//! applied in strict precedence order (later layers win):
//! `CompositionDefault → ChannelDefault → ChannelFixed → InspectorEdit`.
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
pub mod discovery;
pub mod error;
pub mod group;
pub mod manifest;
pub mod overlay;
pub mod selector;
pub mod staging_copy;

// Explicit re-exports at crate root.
pub use binding::{ChannelBinding, ChannelTarget, DottedPath, validate_channel_bindings};
pub use discovery::{
    DiscoveredChannel, OverlayKind, ResolvedOverlay, channel_folder_path, resolve_channel_overlay,
    scan_channels, scan_groups,
};
pub use error::ChannelError;
pub use group::Group;
pub use manifest::{ChannelManifest, ChannelVars, ManifestHeader, OverlayFile, OverlayHeader};
pub use overlay::{ChannelOverlayResult, apply_channel_overlay};
pub use selector::{LabelSelector, SelectorError};
pub use staging_copy::{
    ReuseDecision, SourceStager, StagingError, StagingPlanEntry, open_source_file,
};
