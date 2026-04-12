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
//!   (exactly one target per file, per LD-16c-4).
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
//! higher-priority layers. The provenance chain is bounded at 4 layers:
//! `CompositionDefault → ChannelDefault → ChannelFixed → InspectorEdit`
//! (LD-16c-8).
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
pub mod error;
pub mod overlay;

// LD-009: explicit re-exports at crate root.
pub use binding::{
    ChannelBinding, ChannelTarget, DottedPath, scan_workspace_channels, validate_channel_bindings,
};
pub use error::ChannelError;
pub use overlay::apply_channel_overlay;
