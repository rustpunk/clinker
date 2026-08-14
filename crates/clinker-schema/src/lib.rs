//! Advisory source-schema parsing, indexing, and analysis for Clinker pipelines.
//!
//! This crate owns `.schema.yaml` file parsing, schema discovery, field
//! indexing, and bounded warnings against linked schemas. It does not admit a
//! pipeline for execution: only `clinker-plan` compilation establishes that
//! result. [`SchemaCoverageReport`] states analyzed, partial, skipped, or
//! failed reach explicitly so callers cannot mistake a heuristic miss for
//! complete analysis.

pub mod discovery;
pub mod model;
pub mod parse;
pub mod report;
pub mod validate;

pub use discovery::{
    WorkspaceSchemaAnalysis, analyze_pipeline_file, analyze_workspace_schemas,
    build_workspace_schema_index,
};
pub use model::*;
pub use parse::{
    SchemaAnalysis, SchemaParseError, analyze_schema, analyze_schema_file, parse_schema,
    parse_schema_file,
};
pub use report::{
    CoverageFacet, CoverageReason, CoverageStatus, ReportLocation, ReportSubject,
    SchemaCoverageReport, SchemaReference,
};
pub use validate::{
    PipelineSchemaAnalysis, SchemaWarning, WarningKind, analyze_pipeline, validate_pipeline,
};
