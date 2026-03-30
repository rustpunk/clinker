//! `GitOps` trait — unified interface for git operations.
//!
//! Read operations use gix (pure Rust). Network operations fall back
//! to the git CLI for reliable credential helper integration.
//! Spec: clinker-kiln-git-addendum.md §G2.2.

use std::path::Path;

use crate::types::*;

/// Error type for git operations.
#[derive(Debug, Clone)]
pub enum GitError {
    /// Repository not found at the given path.
    NoRepo(String),
    /// Git operation failed.
    Operation(String),
    /// CLI command failed.
    Cli(String),
}

impl std::fmt::Display for GitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoRepo(msg) => write!(f, "no git repository: {msg}"),
            Self::Operation(msg) => write!(f, "git error: {msg}"),
            Self::Cli(msg) => write!(f, "git cli error: {msg}"),
        }
    }
}

impl std::error::Error for GitError {}

/// Unified git operations trait.
///
/// Implementations: `GixOps` (pure Rust via gitoxide) for read operations,
/// `CliOps` for network operations where credential helpers are critical.
pub trait GitOps: Send + Sync {
    /// Compute repository status (branch, ahead/behind, file statuses).
    fn status(&self) -> Result<RepoStatus, GitError>;

    /// List all branches.
    fn branches(&self) -> Result<Vec<BranchInfo>, GitError>;

    /// Get the current branch name.
    fn current_branch(&self) -> Result<String, GitError>;

    /// Get commit log (most recent first).
    fn log(&self, max: usize) -> Result<Vec<CommitInfo>, GitError>;

    /// Get blame for a file.
    fn blame(&self, path: &Path) -> Result<Vec<BlameLine>, GitError>;
}
