//! `CompileContext` — per-compile-invocation input bag.
//!
//! Threads workspace root and future compile-time options through
//! [`crate::config::PipelineConfig::compile`] without relying on
//! thread-local state or ambient CWD lookups inside the compile pipeline.
//!
//! Production callers resolve `workspace_root` **once** at the entry point
//! (CLI flag, `.clinker.toml` discovery walk, or explicit argument) and
//! pass the resulting context down. Tests use [`CompileContext::default`]
//! which reads CWD at call time — a convenience that is explicitly not
//! sanctioned for production callers.
//!
//! Field cap: 5. If growth pressure exceeds that, split the context
//! into purpose-specific bags rather than letting it sprawl.

use std::path::{Path, PathBuf};

use crate::overlay_ops::LayeredOp;

/// Per-compile configuration threaded through `PipelineConfig::compile`.
#[derive(Debug, Clone)]
pub struct CompileContext {
    /// Absolute, canonicalized path to the workspace root.
    ///
    /// Used by the `.comp.yaml` workspace scanner and any future compile
    /// stages that need filesystem-relative resolution. Production callers
    /// MUST resolve this once at the entry point.
    pub workspace_root: PathBuf,
    /// Workspace-relative directory of the pipeline file being compiled.
    /// Used to resolve relative `use:` paths on `PipelineNode::Composition`
    /// nodes. Empty path means "workspace root" (pipeline file at root).
    pub pipeline_dir: PathBuf,
    /// When `true`, absolute paths in YAML config are permitted.
    /// Wired from the `--allow-absolute-paths` CLI flag and/or
    /// the `CLINKER_ALLOW_ABSOLUTE_PATHS` env var.
    pub allow_absolute_paths: bool,
    /// Resolved structural overlay ops for the channel/group multi-tenant
    /// system, already concatenated across every applicable layer (each op
    /// still tagged with its own [`OverlayLayer`](crate::overlay_ops::OverlayLayer)
    /// so the engine can re-establish the fixed layer-precedence order).
    ///
    /// When non-empty, `compile` splices these into a working copy of the
    /// base AST before schema binding, so the effective (post-overlay) DAG
    /// is what gets typechecked and lowered. Empty (the default) means "no
    /// overlay": the compile path is byte-identical to a plain pipeline.
    pub overlay_ops: Vec<LayeredOp>,
}

impl CompileContext {
    /// Build a context from an explicit workspace root. The caller is
    /// responsible for canonicalization when the path matters for
    /// cross-file identity (e.g. symbol table keying).
    pub fn new(workspace_root: impl Into<PathBuf>) -> Self {
        Self {
            workspace_root: workspace_root.into(),
            pipeline_dir: PathBuf::new(),
            allow_absolute_paths: false,
            overlay_ops: Vec::new(),
        }
    }

    /// Build a context with both workspace root and pipeline directory.
    pub fn with_pipeline_dir(
        workspace_root: impl Into<PathBuf>,
        pipeline_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            workspace_root: workspace_root.into(),
            pipeline_dir: pipeline_dir.into(),
            allow_absolute_paths: false,
            overlay_ops: Vec::new(),
        }
    }

    /// Workspace root accessor.
    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    /// A copy of this context with the overlay ops cleared.
    ///
    /// The pre-compile overlay pass applies the ops to a working copy of the
    /// AST and then compiles that effective plan through the ordinary path;
    /// it uses this to recurse without re-applying (and thus doubling) the
    /// ops it already spliced in.
    pub fn without_overlay_ops(&self) -> Self {
        // Clone only the cheap fields; the whole point is to drop the ops, so
        // copying them via `..self.clone()` just to discard them is wasted work.
        Self {
            workspace_root: self.workspace_root.clone(),
            pipeline_dir: self.pipeline_dir.clone(),
            allow_absolute_paths: self.allow_absolute_paths,
            overlay_ops: Vec::new(),
        }
    }
}

impl Default for CompileContext {
    /// Test convenience only. Reads CWD at call time; production callers
    /// must construct a context explicitly.
    fn default() -> Self {
        Self {
            workspace_root: std::env::current_dir().unwrap_or_default(),
            pipeline_dir: PathBuf::new(),
            allow_absolute_paths: false,
            overlay_ops: Vec::new(),
        }
    }
}
