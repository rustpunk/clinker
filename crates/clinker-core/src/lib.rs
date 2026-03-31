pub mod config;
pub mod dlq;
pub mod error;
pub mod executor;
pub mod exit_codes;
mod integration_tests;
pub mod metrics;
pub mod modules;
pub mod pipeline;
pub mod plan;
pub mod progress;
pub mod projection;
pub mod schema;

#[allow(unused_imports)]
use clinker_record::{PipelineCounters, Record, RecordProvenance, Schema, Value};
