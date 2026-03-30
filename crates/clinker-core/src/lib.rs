pub mod composition;
pub mod config;
pub mod dlq;
pub mod error;
pub mod executor;
pub mod exit_codes;
mod integration_tests;
pub mod log_dispatch;
pub mod log_rules;
pub mod log_template;
pub mod metrics;
pub mod modules;
pub mod pipeline;
pub mod plan;
pub mod progress;
pub mod projection;
pub mod schema;
pub mod security;
pub mod validation;

#[allow(unused_imports)]
use clinker_record::{PipelineCounters, Record, RecordProvenance, Schema, Value};
