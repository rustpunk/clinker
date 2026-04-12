pub mod aggregation;
pub mod config;
pub mod dlq;
pub mod error;
pub mod executor;
pub mod exit_codes;
pub mod graph;
mod integration_tests;
pub mod log_dispatch;
pub mod log_rules;
pub mod log_template;
pub mod metrics;
pub mod modules;
pub mod partial;
pub mod pipeline;
pub mod plan;
pub mod progress;
pub mod projection;
pub mod schema;
pub mod security;
pub mod span;
pub mod validation;
pub mod yaml;

#[cfg(test)]
pub mod test_helpers;

pub use config::{
    CompileContext, CompositionFile, CompositionSignature, CompositionSymbolTable, NodeRef,
    OutputAlias, ParamDecl, ParamName, ParamType, PortDecl, PortName, ResourceDecl, ResourceKind,
    ResourceName, SourceMap, SpannedNodeRef, WORKSPACE_COMPOSITION_BUDGET,
    scan_workspace_signatures, validate_signatures,
};
pub use executor::stage_metrics::{StageCollector, StageMetrics, StageName};
pub use plan::{BoundBody, ColumnLookup, CompositionBodyId, Row, RowTail, TailVarId};

#[allow(unused_imports)]
use clinker_record::{PipelineCounters, Record, RecordProvenance, Schema, Value};
