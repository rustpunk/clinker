//! Post-convergence flush.
//!
//! Drains `correlation_buffers` to writers (clean groups) or the DLQ
//! (dirty groups) per the resolved
//! [`crate::config::CorrelationFanoutPolicy`]. The buffer state at
//! entry is the converged set: forward-pass strict-Output admissions
//! plus the FINAL cascading-retraction iteration's deferred-Output
//! admissions (earlier iterations' deferred records were wiped at the
//! top of each iteration via the orchestrator's baseline restore).
//! Both strict and relaxed pipelines fan in here through the same
//! [`commit_correlation_buffers`] body — the per-group emission shape
//! is shared; only the path that populates the buffer differs.

use super::detect::RetractScope;
use crate::error::PipelineError;
use crate::executor::dispatch::{ExecutorContext, commit_correlation_buffers};

/// Drain the converged correlation buffer to writers and DLQ.
///
/// Each writer opens, accepts every record from its `output_name`
/// queue, then flushes and closes — exactly once per Output per
/// pipeline run. Per-iteration writer flushes would emit duplicates
/// when a later iteration retracts an earlier-iteration emit; the
/// orchestrator's baseline restore ensures that by the time this body
/// runs, every deferred-Output queue holds only the converged set.
pub(crate) fn flush_buffered(
    ctx: &mut ExecutorContext<'_>,
    _scope: &RetractScope,
) -> Result<(), PipelineError> {
    commit_correlation_buffers(ctx)
}
