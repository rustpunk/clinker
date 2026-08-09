//! Runtime execution for admitted Clinker plans.

pub mod aggregation;
pub mod dlq;
pub mod executor;
pub mod exit_codes;
mod integration_tests;
mod log_dispatch;
pub mod metrics;
pub mod output;
pub mod partial;
pub mod pipeline;
pub mod progress;
pub mod projection;
pub mod sketch;
pub mod source;
pub mod telemetry;

pub use executor::stage_metrics::{StageCollector, StageMetrics, StageName};
