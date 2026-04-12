pub mod binding;
pub mod error;

// LD-009: explicit re-exports at crate root.
pub use binding::{
    ChannelBinding, ChannelTarget, DottedPath, scan_workspace_channels, validate_channel_bindings,
};
pub use error::ChannelError;
