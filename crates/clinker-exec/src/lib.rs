//! Runtime execution for admitted Clinker plans.

pub mod aggregation;
pub mod dlq;
pub mod executor;
pub mod exit_codes;
mod integration_tests;
mod log_dispatch;
#[cfg(feature = "test-utils")]
pub use log_dispatch::dispatch_compiled_transform_logs_for_testing;
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

#[cfg(test)]
mod test_support;
