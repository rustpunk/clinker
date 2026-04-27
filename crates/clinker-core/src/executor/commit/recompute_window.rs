//! Phase 3: recompute window partitions.
//!
//! No-op stub for the universe of pipelines Phase 6 accepts: every
//! pipeline lacking a relaxed-window aggregate downstream of a relaxed-
//! CK aggregate produces zero window deltas, which is what this body
//! returns. Wired so the orchestrator's shape is stable when the
//! analytic-window retraction path lands later — the call site does not
//! change, only this body grows from `Vec::new()` into the real per-
//! partition recompute loop.
//!
//! The signature carries the right shape (consumes the
//! [`super::detect::RetractScope`]'s window list, returns
//! `Vec<super::Delta>`) so the call site is binding-stable.

use super::{Delta, detect::RetractScope};
use crate::error::PipelineError;
use crate::executor::dispatch::ExecutorContext;

/// Always returns an empty delta list under the Phase 6 universe. A
/// non-empty `scope.windows` here would imply the planner has been
/// extended to mark relaxed window partitions for retraction; the body
/// fills out at that point. Today the detect phase never populates
/// `scope.windows`, so the empty-input branch covers every reachable
/// invocation.
pub(crate) fn recompute_window_partitions(
    _ctx: &mut ExecutorContext<'_>,
    _scope: &RetractScope,
) -> Result<Vec<Delta>, PipelineError> {
    Ok(Vec::new())
}
