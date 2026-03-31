//! Git backend abstraction for Clinker Kiln.
//!
//! Provides a `GitOps` trait with a git CLI implementation. The trait
//! is designed for a future gix (gitoxide) backend to be swapped in
//! for read operations once gix stabilizes on Rust edition 2024.

pub mod gix_backend;
pub mod ops;
pub mod provider;
pub mod types;

pub use gix_backend::GitCliOps;
pub use ops::{GitError, GitOps};
pub use provider::{
    PrParams, PrResult, ProviderKind, create_pr, detect_provider, get_default_branch,
    get_remote_url, parse_remote_url,
};
pub use types::*;
