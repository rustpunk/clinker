//! Runtime execution for admitted Clinker plans.
//!
//! Retired pipeline-authored routing and message interpolation APIs are not
//! part of the executor facade:
//!
//! ```compile_fail
//! use clinker_exec::log_rules::{LogRule, load_log_rules};
//! ```
//!
//! ```compile_fail
//! use clinker_exec::log_template::{LogTemplateContext, resolve_template};
//! ```
//!
//! ```compile_fail
//! use clinker_exec::log_dispatch::LogDispatcher;
//! ```

pub mod aggregation;
pub mod dlq;
pub mod executor;
pub mod exit_codes;
mod integration_tests;
pub mod metrics;
pub mod output;
pub mod partial;
pub mod pipeline;
pub mod progress;
pub mod projection;
pub mod sketch;
pub mod source;

pub use executor::stage_metrics::{StageCollector, StageMetrics, StageName};
