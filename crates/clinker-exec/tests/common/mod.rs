//! Shared fixture for the executor integration tests relocated out of
//! `src/executor/tests/`.
//!
//! These tests drive the full [`PipelineExecutor`] against in-memory CSV
//! readers/writers. They reach the executor through its public
//! `&CompiledPlan` entry point
//! ([`PipelineExecutor::run_plan_with_readers_writers`]) — the same
//! surface every other integration test in this directory uses — so no
//! crate-private executor seam is exercised from out here.
//!
//! Only [`run_config`] is exported: it is the single primitive every
//! relocated single/multi-source pipeline test routes its run through, so
//! every test binary that includes this module references it and nothing
//! here goes unused. Tests that merely compile-and-inspect a plan, or that
//! call the public entry point directly with a bespoke reader set, do not
//! include this module at all.

use std::collections::HashMap;
use std::io::Write;

use clinker_exec::executor::{ExecutionReport, PipelineExecutor, PipelineRunParams, SourceReaders};
use clinker_plan::config::{CompileContext, PipelineConfig};
use clinker_plan::error::PipelineError;

/// Run a parsed `config` to completion against the supplied readers and
/// writers, returning the full [`ExecutionReport`].
///
/// Mirrors the crate-internal `run_with_readers_writers` the relocated
/// tests previously called: it compiles the config against the default
/// (CWD-anchored) context and forwards to the public `&CompiledPlan`
/// entry point, which itself re-derives the run from `plan.config()` — so
/// the path is behaviorally identical to the old crate-private call. The
/// in-memory-reader tests have no on-disk sources, so volume estimates
/// collapse to `0` and dispatch falls back to topological order, exactly
/// as before. A compile failure is a malformed-fixture bug and panics
/// rather than masquerading as a runtime error.
pub fn run_config(
    config: &PipelineConfig,
    readers: SourceReaders,
    writers: HashMap<String, Box<dyn Write + Send>>,
    params: &PipelineRunParams,
) -> Result<ExecutionReport, PipelineError> {
    let plan = config
        .compile(&CompileContext::default())
        .expect("integration-test pipeline must compile");
    PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, params)
}
