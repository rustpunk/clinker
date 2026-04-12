pub mod binding;
pub mod error;

// LD-009: explicit re-exports at crate root.
pub use binding::{ChannelBinding, ChannelTarget, DottedPath};
pub use error::ChannelError;
