//! Runtime-failure vocabulary the top-level [`PipelineError`](crate::error::PipelineError)
//! aggregates.
//!
//! [`SpillError`] and [`BudgetCategory`] are leaf enums produced by the
//! execution engine's disk-spill and memory-budget subsystems, but they are
//! defined here, alongside the error type that names them, so the planning
//! layer can own the unified `PipelineError` without depending upward on the
//! executor.

/// Disk-spill I/O or decode failure.
///
/// Surfaced by the spill reader/writer in the execution layer. The decode
/// variants preserve the underlying postcard / JSON-header context that a
/// bare [`std::io::Error`] would lose.
#[derive(Debug)]
pub enum SpillError {
    Io(std::io::Error),
    Json(serde_json::Error),
    Postcard(postcard::Error),
    InvalidSchema(String),
    /// The spill root directory became unusable mid-run: it was removed,
    /// unmounted, remounted read-only, or had its permissions revoked after
    /// the run validated it at startup. Distinct from [`SpillError::Io`] so
    /// the rendered diagnostic points at the directory and its likely cause
    /// (an NFS remount, a volume unmount, an over-eager temp-file cleaner)
    /// rather than reading as a generic byte-stream I/O failure. Carries the
    /// offending directory path and the underlying OS message.
    DirUnavailable { dir: String, source: String },
}

impl std::fmt::Display for SpillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpillError::Io(e) => write!(f, "spill I/O error: {e}"),
            SpillError::Json(e) => write!(f, "spill JSON header error: {e}"),
            SpillError::Postcard(e) => write!(f, "spill postcard error: {e}"),
            SpillError::InvalidSchema(msg) => write!(f, "spill schema error: {msg}"),
            SpillError::DirUnavailable { dir, source } => write!(
                f,
                "spill directory {dir} became unavailable mid-run: {source} \
                 (the directory may have been unmounted, remounted read-only, \
                 deleted by an external cleaner, or had its permissions revoked)"
            ),
        }
    }
}

impl SpillError {
    /// Classify an [`std::io::Error`] raised while creating a spill file in
    /// the spill root directory.
    ///
    /// A failure to create a temp file in a directory the run validated as
    /// writable at startup means the directory itself went bad mid-run
    /// (`NotFound` → removed/unmounted, `PermissionDenied`/`ReadOnlyFilesystem`
    /// → permissions revoked or read-only remount). Those map to the distinct
    /// [`SpillError::DirUnavailable`] so the operator sees a directory-level
    /// cause rather than a generic I/O error. Any other kind (a genuine
    /// byte-stream fault, `ENOSPC`, etc.) stays [`SpillError::Io`].
    pub fn from_spill_dir_io(dir: &std::path::Path, e: std::io::Error) -> Self {
        use std::io::ErrorKind;
        match e.kind() {
            ErrorKind::NotFound | ErrorKind::PermissionDenied | ErrorKind::ReadOnlyFilesystem => {
                SpillError::DirUnavailable {
                    dir: dir.display().to_string(),
                    source: e.to_string(),
                }
            }
            _ => SpillError::Io(e),
        }
    }
}

impl std::error::Error for SpillError {}

impl From<std::io::Error> for SpillError {
    fn from(e: std::io::Error) -> Self {
        SpillError::Io(e)
    }
}

impl From<serde_json::Error> for SpillError {
    fn from(e: serde_json::Error) -> Self {
        SpillError::Json(e)
    }
}

impl From<postcard::Error> for SpillError {
    fn from(e: postcard::Error) -> Self {
        SpillError::Postcard(e)
    }
}

impl From<lz4_flex::frame::Error> for SpillError {
    fn from(e: lz4_flex::frame::Error) -> Self {
        SpillError::Io(std::io::Error::other(e.to_string()))
    }
}

/// Diagnostic tag for a memory budget overrun.
///
/// All categories charge the same global limit counter; the tag classifies
/// which allocation class tripped it, for diagnostics and downstream
/// routing only.
///
/// Append-only. Removing a variant is a breaking change for any
/// `MemoryBudgetExceeded` consumer that destructures `source`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum BudgetCategory {
    /// Source-rooted arenas, node-rooted arenas, deferred-region
    /// admission buffers, grace-hash build/probe accounting, and the
    /// disk-spill quota counter. Every budget-tracked allocation that
    /// is not `ctx.node_buffers` falls under this tag.
    Arena,
    /// `ctx.node_buffers` — the inter-stage handoff layer between
    /// non-fused operators. Each slot registers a `NodeBufferConsumer`
    /// wrapper; the arbitrator's pull-mode `current_usage` reads the
    /// slot's live footprint at every policy poll.
    NodeBuffer,
}

impl std::fmt::Display for BudgetCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Arena => f.write_str("arena"),
            Self::NodeBuffer => f.write_str("node_buffer"),
        }
    }
}
