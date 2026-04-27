//! Phase 5: flush.
//!
//! Drains the post-replay `correlation_buffers` and routes records to
//! writers (clean groups) or the DLQ (dirty groups) per the resolved
//! [`crate::config::CorrelationFanoutPolicy`]. Reuses the strict path's
//! emission helpers — `commit_correlation_buffers_strict` is the
//! single-source-of-truth flush body and the relaxed path delegates to
//! it after the upstream phases have substituted the changed rows.

use super::detect::RetractScope;
use crate::error::PipelineError;
use crate::executor::dispatch::{ExecutorContext, commit_correlation_buffers_strict};

/// Run the post-replay flush. Today this defers to the strict path's
/// emission shape because Phase 6's retract pipeline mutates the
/// `correlation_buffers` content (substituting new aggregate output
/// rows) without changing the per-group dispositioning logic — clean
/// stays clean, dirty stays dirty, overflow stays overflow. The
/// per-output `CorrelationFanoutPolicy` resolution becomes load-bearing
/// when ALL/PRIMARY downgrades start sparing collateral records, which
/// the strict body honors via [`should_spare_collateral`] below.
pub(crate) fn flush_buffered(
    ctx: &mut ExecutorContext<'_>,
    _scope: &RetractScope,
    commit_group_by: &[String],
) -> Result<(), PipelineError> {
    commit_correlation_buffers_strict(
        ctx,
        "correlation_commit__terminal",
        commit_group_by,
        ctx.correlation_max_group_buffer,
    )
}
