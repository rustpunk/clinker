//! The shape lineage leaves behind in a build compiled without it.
//!
//! [`LiveLineageOutput`] is uninhabited, so `Option<LiveLineageOutput>` is
//! statically `None` and every run path that emits a lifecycle event or closes
//! a sink compiles unchanged and does nothing. The alternative was a `#[cfg]`
//! at each of those places, which would put the reader of the run path — the
//! code that has to be read to know when a terminal is written — in the
//! business of knowing which build it is in.
//!
//! Nothing here can be reached: `capability::check_lineage_requested` refuses
//! `--lineage` and `--lineage-events` before a run gets this far, so no value
//! of this type is ever asked for.

use crate::lifecycle::{RunLifecycleSnapshot, RunLifecycleStartFacts};
use clinker_plan::error::PipelineError;

/// A live lineage destination in a build that cannot open one.
pub(crate) enum LiveLineageOutput {}

impl LiveLineageOutput {
    pub(crate) fn emit_start(&mut self, _start: &RunLifecycleStartFacts) {
        match *self {}
    }

    pub(crate) fn emit_terminal(
        &mut self,
        _snapshot: &RunLifecycleSnapshot,
    ) -> Result<(), PipelineError> {
        match *self {}
    }
}

/// Close a live lineage output. There is never one to close.
pub(crate) fn finish_live_lineage(_output: &mut Option<LiveLineageOutput>) {}
