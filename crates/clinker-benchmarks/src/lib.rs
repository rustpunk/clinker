//! End-to-end benchmark runner and reporters for Clinker pipelines.
//!
//! Breaks the circular dependency between `clinker-core` and `clinker-bench-support`
//! by housing the runner (which needs both) in its own crate.

pub mod format_mapping;
pub mod report;
pub mod runner;
