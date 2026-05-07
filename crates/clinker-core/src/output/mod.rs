//! Output-side helpers for collision policy, path templating, and
//! provenance sidecars.
//!
//! Shared by the non-split sink path in `clinker::main` and the split
//! file factory in `executor::build_format_writer`. Lives in
//! `clinker-core` so both the binary crate and the executor can reach it
//! without re-implementing the policy.

pub mod open;
pub mod path_template;
pub mod sidecar;
