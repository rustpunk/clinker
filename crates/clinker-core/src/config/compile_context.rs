//! `CompileContext` — per-compile-invocation input bag (LD-16c-12).
//!
//! Threads workspace root and future compile-time options through
//! [`crate::config::PipelineConfig::compile`] without relying on
//! thread-local state or ambient CWD lookups inside the compile pipeline.
//!
//! Production callers resolve `workspace_root` **once** at the entry point
//! (CLI flag, `.clinker.toml` discovery walk, or explicit argument) and
//! pass the resulting context down. Tests use [`CompileContext::default`]
//! which reads CWD at call time — a convenience that is explicitly not
//! sanctioned for production callers per LD-16c-12.
//!
//! Field cap: 5. If growth pressure exceeds that, split the context
//! per LD-16c-12's god-object guard rather than letting it sprawl.

use std::path::{Path, PathBuf};

/// Per-compile configuration threaded through `PipelineConfig::compile`.
#[derive(Debug, Clone)]
pub struct CompileContext {
    /// Absolute, canonicalized path to the workspace root.
    ///
    /// Used by the Phase 1 `.comp.yaml` scanner and any future compile
    /// stages that need filesystem-relative resolution. Production callers
    /// MUST resolve this once at the entry point.
    pub workspace_root: PathBuf,
}

impl CompileContext {
    /// Build a context from an explicit workspace root. The caller is
    /// responsible for canonicalization when the path matters for
    /// cross-file identity (e.g. symbol table keying).
    pub fn new(workspace_root: impl Into<PathBuf>) -> Self {
        Self {
            workspace_root: workspace_root.into(),
        }
    }

    /// Workspace root accessor.
    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }
}

impl Default for CompileContext {
    /// Test convenience only. Reads CWD at call time; production callers
    /// must construct a context explicitly.
    fn default() -> Self {
        Self {
            workspace_root: std::env::current_dir().unwrap_or_default(),
        }
    }
}
